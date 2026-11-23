package de.ids_mannheim.korapxmltools

import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

/**
 * Updates an existing Krill tar from new inputs by streaming it entry by entry
 * ("merge mode", see KrillJsonPatcher).
 *
 * Entries that are not affected by the update are copied through without even
 * being decompressed, so they stay byte-identical and cost only I/O. Affected
 * entries are decompressed, patched and recompressed on a worker pool while the
 * main thread keeps reading and writing, preserving the original entry order
 * with bounded memory (at most a few texts in flight).
 *
 * The output is always a new tar file; the input tar is never modified, so an
 * interrupted merge cannot damage existing data.
 */
class KrillTarMerger(
    private val logger: Logger,
    private val threads: Int,
    private val maxPatchBytes: Int = MAX_PATCH_BYTES
) {
    companion object {
        /** Strings with non-Latin-1 characters hold at most 2^30 - 1 chars, and UTF-8 has at least one byte per char. */
        const val MAX_PATCH_BYTES = (1 shl 30) - 1
    }

    fun interface TextPatcher {
        /**
         * Return a JSON patch function for the tar entry with this normalized text id
         * (the entry base name, e.g. "REI-RBR-00473"), or null when the text is not
         * affected and its bytes should be copied through unchanged.
         */
        fun patcherFor(normalizedTextId: String): ((String) -> String)?
    }

    data class Stats(
        var entries: Int = 0,
        var patched: Int = 0,
        var copied: Int = 0,
        val seenTextIds: MutableSet<String> = mutableSetOf(),
        /** Entries too large to patch in memory, copied unchanged. */
        val tooLarge: MutableList<String> = java.util.Collections.synchronizedList(mutableListOf())
    )

    private class OutEntry(val name: String, val modTime: Long, val bytes: Future<ByteArray>)

    /**
     * Stream [inputTar] to [outputTar], patching affected texts via [patcher].
     * [onProgress] is called after each written entry with the number of bytes
     * read from the input tar so far.
     */
    fun merge(
        inputTar: File,
        outputTar: File,
        patcher: TextPatcher,
        onProgress: ((bytesRead: Long) -> Unit)? = null
    ): Stats {
        val stats = Stats()
        val pool: ExecutorService = Executors.newFixedThreadPool(threads.coerceAtLeast(1)) { r ->
            Thread(r, "KrillMergeWorker").apply { isDaemon = true }
        }
        // Bounded number of in-flight patch jobs keeps memory flat on huge tars.
        val maxInFlight = (threads.coerceAtLeast(1)) * 2 + 1
        val pending = ArrayDeque<OutEntry>()

        try {
            TarArchiveInputStream(BufferedInputStream(FileInputStream(inputTar), 1 shl 20)).use { tarIn ->
                TarArchiveOutputStream(BufferedOutputStream(FileOutputStream(outputTar), 1 shl 20)).use { tarOut ->
                    tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX)

                    fun drainOne() {
                        val out = pending.removeFirst()
                        val bytes = out.bytes.get()
                        val entry = TarArchiveEntry(out.name)
                        entry.size = bytes.size.toLong()
                        entry.setModTime(out.modTime)
                        tarOut.putArchiveEntry(entry)
                        tarOut.write(bytes)
                        tarOut.closeArchiveEntry()
                        onProgress?.invoke(tarIn.bytesRead)
                    }

                    var entry: TarArchiveEntry? = tarIn.nextEntry
                    while (entry != null) {
                        if (!entry.isFile) {
                            entry = tarIn.nextEntry
                            continue
                        }
                        stats.entries++
                        val name = entry.name
                        val baseName = name.substringAfterLast('/')
                        val compression = when {
                            baseName.endsWith(".json.gz") -> Compression.GZIP
                            baseName.endsWith(".json.lz4") -> Compression.LZ4
                            else -> null
                        }
                        val normalizedId = when (compression) {
                            Compression.GZIP -> baseName.removeSuffix(".json.gz")
                            Compression.LZ4 -> baseName.removeSuffix(".json.lz4")
                            null -> null
                        }
                        if (normalizedId != null) {
                            stats.seenTextIds.add(normalizedId)
                        }
                        val patchFn = normalizedId?.let { patcher.patcherFor(it) }
                        // The entry's bytes must be consumed before advancing to the next
                        // entry, so reading happens here; only patching is parallel.
                        val raw = tarIn.readAllBytes()
                        val modTime = entry.lastModifiedDate.time

                        if (patchFn == null || compression == null) {
                            stats.copied++
                            pending.addLast(OutEntry(name, modTime, java.util.concurrent.CompletableFuture.completedFuture(raw)))
                        } else {
                            stats.patched++
                            pending.addLast(OutEntry(name, modTime, pool.submit<ByteArray> {
                                patchOrCopy(name, raw, compression, patchFn, stats)
                            }))
                        }
                        while (pending.size >= maxInFlight) drainOne()
                        entry = tarIn.nextEntry
                    }
                    while (pending.isNotEmpty()) drainOne()
                    tarOut.finish()
                }
            }
        } finally {
            pool.shutdownNow()
        }
        return stats
    }

    private enum class Compression { GZIP, LZ4 }

    /**
     * Patched entry bytes, or the unchanged [raw] bytes when the text's JSON is too
     * large for a Java String (very long books): one such text must not fail the tar.
     * shortcut: such texts keep their old annotations; upgrade if they matter, by patching on bytes.
     */
    private fun patchOrCopy(
        name: String, raw: ByteArray, compression: Compression, patchFn: (String) -> String, stats: Stats
    ): ByteArray {
        fun tooLarge(reason: String): ByteArray {
            stats.tooLarge.add(name)
            logger.warning("Text $name is too large to patch ($reason); copied unchanged")
            return raw
        }
        val json = decompress(raw, compression) ?: return tooLarge("over $maxPatchBytes bytes of JSON")
        return try {
            recompress(patchFn(json), compression)
        } catch (e: OutOfMemoryError) {
            // String/array length limits, not an exhausted heap
            val msg = e.message ?: ""
            if (msg.contains("Java heap space") || msg.contains("GC overhead")) throw e
            tooLarge(msg)
        }
    }

    /** Decompressed JSON, or null when it has more than [maxPatchBytes] bytes. */
    private fun decompress(bytes: ByteArray, compression: Compression): String? {
        val input = when (compression) {
            Compression.GZIP -> GZIPInputStream(ByteArrayInputStream(bytes))
            Compression.LZ4 -> net.jpountz.lz4.LZ4FrameInputStream(ByteArrayInputStream(bytes))
        }
        return input.use {
            val data = it.readNBytes(maxPatchBytes + 1)
            if (data.size > maxPatchBytes) null else data.toString(StandardCharsets.UTF_8)
        }
    }

    private fun recompress(json: String, compression: Compression): ByteArray {
        val byteOut = ByteArrayOutputStream(json.length / 2)
        val out = when (compression) {
            // Same gzip level as KorapXmlTool.compressKrillJson, so patched entries
            // match what a fresh krill export would produce.
            Compression.GZIP -> object : GZIPOutputStream(byteOut) {
                init {
                    def.setLevel(1)
                }
            }
            Compression.LZ4 -> net.jpountz.lz4.LZ4FrameOutputStream(byteOut)
        }
        out.use { it.write(json.toByteArray(StandardCharsets.UTF_8)) }
        return byteOut.toByteArray()
    }
}
