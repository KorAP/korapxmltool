package de.ids_mannheim.korapxmltools

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import java.util.zip.GZIPInputStream
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * Tests for Krill tar merge mode: updating an existing Krill tar from
 * KorAP-XML ZIPs and stand-off metadata files.
 */
class KrillTarMergeTest {

    companion object {
        private val tempDirs = mutableListOf<File>()

        private fun newTempDir(key: String): File =
            File.createTempFile(key, "").apply {
                delete()
                mkdirs()
                tempDirs.add(this)
            }

        // Generated once and shared between tests (inputs are never modified)
        private val baseTar: File by lazy { generateTar("merge_base", "rei_sample.krill.tar", resource("rei_sample.zip")) }
        private val ttTar: File by lazy {
            generateTar("merge_tt", "rei_sample.krill.tar", resource("rei_sample.zip"), resource("rei_sample.tree_tagger.zip"))
        }

        private fun resource(path: String): String {
            val url: URL = Thread.currentThread().contextClassLoader.getResource(path)
                ?: throw IllegalArgumentException("Resource $path not found")
            return File(url.toURI()).path
        }

        private fun generateTar(key: String, tarName: String, vararg inputs: String): File {
            val outputDir = newTempDir(key)
            val exitCode = debug(arrayOf("-t", "krill", "-q", "-D", outputDir.path) + inputs)
            assertEquals(0, exitCode, "Krill conversion should succeed for '$key'")
            val tar = File(outputDir, tarName)
            assertTrue(tar.exists(), "Expected $tarName for '$key'")
            return tar
        }

        /** Raw (still compressed) tar entries by name, in order. */
        private fun readTarEntries(tar: File): LinkedHashMap<String, ByteArray> {
            val entries = LinkedHashMap<String, ByteArray>()
            TarArchiveInputStream(tar.inputStream().buffered()).use { tarIn ->
                var entry = tarIn.nextEntry
                while (entry != null) {
                    if (entry.isFile) {
                        entries[entry.name] = tarIn.readAllBytes()
                    }
                    entry = tarIn.nextEntry
                }
            }
            return entries
        }

        /** Decompressed JSON entries by name, in order. */
        private fun readTarJson(tar: File): LinkedHashMap<String, String> {
            val result = LinkedHashMap<String, String>()
            readTarEntries(tar).forEach { (name, bytes) ->
                if (name.endsWith(".json.gz")) {
                    result[name] = GZIPInputStream(ByteArrayInputStream(bytes)).bufferedReader().use { it.readText() }
                }
            }
            return result
        }

        @JvmStatic
        @AfterClass
        fun cleanupTempDirs() {
            tempDirs.forEach { it.deleteRecursively() }
            tempDirs.clear()
        }
    }

    private val outContent = ByteArrayOutputStream(10000000)
    private val errContent = ByteArrayOutputStream()
    private val originalOut: PrintStream = System.out
    private val originalErr: PrintStream = System.err

    @Before
    fun setUpStreams() {
        System.setOut(PrintStream(outContent))
        System.setErr(PrintStream(errContent))
    }

    @After
    fun restoreStreams() {
        System.setOut(originalOut)
        System.setErr(originalErr)
    }

    private fun mergeTar(key: String, inputTar: File, vararg inputs: String): Pair<File, File> {
        val outputDir = newTempDir(key)
        val exitCode = debug(arrayOf("-t", "krill", "-q", "-D", outputDir.path, inputTar.path) + inputs)
        assertEquals(0, exitCode, "Krill tar merge should succeed for '$key'")
        val outputs = outputDir.listFiles { f -> f.name.endsWith(".tar") }.orEmpty()
        assertEquals(1, outputs.size, "Expected exactly one merged tar for '$key'")
        val log = File(outputs[0].path.replace(Regex("\\.tar$"), ".log"))
        return outputs[0] to log
    }

    // ------------------------------------------------------------------
    // Phase 1: metadata merge
    // ------------------------------------------------------------------

    @Test
    fun mergedStandoffMetadataMatchesFullGeneration() {
        val standoff = resource("rei_sample.domains.meta.xml")
        val full = generateTar("standoff_full", "rei_sample.krill.tar", resource("rei_sample.zip"), standoff)
        val (merged, _) = mergeTar("standoff_merge", baseTar, standoff)

        val fullJson = readTarJson(full)
        val mergedJson = readTarJson(merged)
        // Full generation writes texts in compression-completion order, which is not
        // deterministic; only the set of entries and their contents must match.
        assertEquals(fullJson.keys, mergedJson.keys, "Entry names should match")
        fullJson.forEach { (name, expected) ->
            assertEquals(expected, mergedJson[name], "Merged JSON should equal full generation for $name")
        }
        assertContains(mergedJson.values.first(), "wikiDomain")
    }

    @Test
    fun mergeLeavesUnaffectedTextsByteIdentical() {
        // Stand-off metadata for a single text only
        val standoffFile = File(newTempDir("standoff_partial_input"), "partial.meta.xml")
        standoffFile.writeText(
            """
            <standOff xmlns="http://www.tei-c.org/ns/1.0">
              <metadataLayer xml:id="testDomain" type="classification">
                <taxonomy xml:id="testtaxonomy">
                  <category xml:id="Politics"><catDesc>Politics</catDesc></category>
                </taxonomy>
                <textRef target="REI_RBR.00473">
                  <catRef scheme="#testtaxonomy" target="#Politics" n="1" cert="0.9"/>
                </textRef>
              </metadataLayer>
            </standOff>
            """.trimIndent()
        )
        val (merged, _) = mergeTar("standoff_partial", baseTar, standoffFile.path)

        val baseEntries = readTarEntries(baseTar)
        val mergedEntries = readTarEntries(merged)
        assertEquals(baseEntries.keys.toList(), mergedEntries.keys.toList())

        baseEntries.forEach { (name, bytes) ->
            if (name.startsWith("REI-RBR-00473")) {
                assertFalse(bytes.contentEquals(mergedEntries[name]!!), "$name should have been patched")
            } else {
                assertTrue(bytes.contentEquals(mergedEntries[name]!!),
                    "$name should be copied through byte-identically")
            }
        }
        val patched = readTarJson(merged).getValue("REI-RBR-00473.json.gz")
        assertContains(patched, "\"key\":\"testDomain\"")
        assertContains(patched, "\"Politics\"")
    }

    @Test
    fun mergeReplacesExistingStandoffField() {
        // First give all texts wikiDomain fields, then merge an updated classification
        // for one text and check it replaces (not duplicates) the old field.
        val withDomains = generateTar(
            "standoff_replace_base", "rei_sample.krill.tar",
            resource("rei_sample.zip"), resource("rei_sample.domains.meta.xml")
        )
        val updateFile = File(newTempDir("standoff_replace_input"), "update.meta.xml")
        updateFile.writeText(
            """
            <standOff xmlns="http://www.tei-c.org/ns/1.0">
              <metadataLayer xml:id="wikiDomain" type="classification">
                <taxonomy xml:id="wikitaxonomy">
                  <category xml:id="UpdatedTopic"><catDesc>Updated topic</catDesc></category>
                </taxonomy>
                <textRef target="REI_RBR.00473">
                  <catRef scheme="#wikitaxonomy" target="#UpdatedTopic" n="1" cert="0.99"/>
                </textRef>
              </metadataLayer>
            </standOff>
            """.trimIndent()
        )
        val (merged, _) = mergeTar("standoff_replace", withDomains, updateFile.path)
        val patched = readTarJson(merged).getValue("REI-RBR-00473.json.gz")
        assertContains(patched, "UpdatedTopic")
        assertEquals(1, Regex("\"key\":\"wikiDomain\"").findAll(patched).count(),
            "wikiDomain must be replaced, not duplicated")
    }

    @Test
    fun mergeWarnsAndIgnoresTextsNotInTar() {
        val wud24Tar = File(resource("wud24_sample.krill.tar"))
        val (merged, log) = mergeTar("ignore_missing", wud24Tar, resource("rei_sample.tree_tagger.zip"))

        // Nothing to patch: all entries must be byte-identical copies
        val inEntries = readTarEntries(wud24Tar)
        val outEntries = readTarEntries(merged)
        assertEquals(inEntries.keys.toList(), outEntries.keys.toList())
        inEntries.forEach { (name, bytes) ->
            assertTrue(bytes.contentEquals(outEntries[name]!!), "$name should be unchanged")
        }

        assertTrue(log.exists(), "Merge log file should exist")
        val logText = log.readText()
        assertContains(logText, "Ignoring text REI_RBR.00473")
        assertContains(logText, "not present in")
    }

    // ------------------------------------------------------------------
    // Phase 2: annotation foundry merge
    // ------------------------------------------------------------------

    @Test
    fun mergedTreeTaggerFoundryMatchesFullGeneration() {
        val (merged, _) = mergeTar("tt_merge", baseTar, resource("rei_sample.tree_tagger.zip"))

        val fullJson = readTarJson(ttTar)
        val mergedJson = readTarJson(merged)
        assertEquals(fullJson.keys, mergedJson.keys)
        fullJson.forEach { (name, expected) ->
            assertEquals(expected, mergedJson[name], "Merged JSON should equal full generation for $name")
        }
    }

    @Test
    fun mergedMaltDependenciesMatchFullGeneration() {
        val full = generateTar(
            "malt_full", "rei_sample.krill.tar",
            resource("rei_sample.zip"), resource("rei_sample.malt.zip")
        )
        val (merged, _) = mergeTar("malt_merge", baseTar, resource("rei_sample.malt.zip"))

        val fullJson = readTarJson(full)
        val mergedJson = readTarJson(merged)
        assertEquals(fullJson.keys, mergedJson.keys)
        fullJson.forEach { (name, expected) ->
            assertEquals(expected, mergedJson[name], "Merged JSON should equal full generation for $name")
        }
    }

    @Test
    fun mergedMultipleFoundriesMatchFullGeneration() {
        val full = generateTar(
            "multi_full", "rei_sample.krill.tar",
            resource("rei_sample.zip"), resource("rei_sample.tree_tagger.zip"),
            resource("rei_sample.malt.zip"), resource("rei_sample.opennlp.zip")
        )
        val (merged, _) = mergeTar(
            "multi_merge", baseTar,
            resource("rei_sample.tree_tagger.zip"), resource("rei_sample.malt.zip"),
            resource("rei_sample.opennlp.zip")
        )

        val fullJson = readTarJson(full)
        val mergedJson = readTarJson(merged)
        assertEquals(fullJson.keys, mergedJson.keys)
        fullJson.forEach { (name, expected) ->
            assertEquals(expected, mergedJson[name], "Merged JSON should equal full generation for $name")
        }
    }

    @Test
    fun mergeReplacesExistingFoundry() {
        // Re-merging the same tree_tagger annotations into a tar that already has
        // them must be idempotent (replace, not duplicate).
        val (merged, _) = mergeTar("tt_replace", ttTar, resource("rei_sample.tree_tagger.zip"))

        val fullJson = readTarJson(ttTar)
        val mergedJson = readTarJson(merged)
        assertEquals(fullJson.keys, mergedJson.keys)
        fullJson.forEach { (name, expected) ->
            assertEquals(expected, mergedJson[name], "Replacing a foundry with itself must be idempotent for $name")
        }
    }

    @Test
    fun mergeSupportsLz4CompressedTars() {
        val outputDir = newTempDir("lz4_base")
        val exitCode = debug(arrayOf("-t", "krill", "-q", "--lz4", "-D", outputDir.path, resource("rei_sample.zip")))
        assertEquals(0, exitCode)
        val lz4Tar = File(outputDir, "rei_sample.krill.tar")
        assertTrue(lz4Tar.exists())

        val (merged, _) = mergeTar("lz4_merge", lz4Tar, resource("rei_sample.tree_tagger.zip"))
        val entries = readTarEntries(merged)
        assertTrue(entries.keys.all { it.endsWith(".json.lz4") }, "Entries should stay LZ4-compressed")
        val json = net.jpountz.lz4.LZ4FrameInputStream(
            ByteArrayInputStream(entries.getValue("REI-RBR-00473.json.lz4"))
        ).bufferedReader().use { it.readText() }
        assertContains(json, "tt/p:")
        assertContains(json, "treetagger/morpho")
    }

    @Test
    fun remergingHeadersFromBaseZipIsIdempotent() {
        // The base ZIP carries the same headers the tar was built from, so re-merging
        // it must leave every text's JSON unchanged (data/tokens/structure entries are
        // ignored, header-derived fields regenerate to identical values).
        val (merged, log) = mergeTar("header_remerge", baseTar, resource("rei_sample.zip"))

        val baseJson = readTarJson(baseTar)
        val mergedJson = readTarJson(merged)
        assertEquals(baseJson.keys, mergedJson.keys)
        baseJson.forEach { (name, expected) ->
            assertEquals(expected, mergedJson[name], "Re-merging identical headers must not change $name")
        }
        assertContains(log.readText(), "cannot change the base tokenization")
    }

    // ------------------------------------------------------------------
    // CLI semantics
    // ------------------------------------------------------------------

    @Test
    fun mergeRejectsNonKrillOutputFormat() {
        val exitCode = debug(arrayOf("-t", "conllu", baseTar.path, resource("rei_sample.tree_tagger.zip")))
        assertNotEquals(0, exitCode, "Tar input without -t krill must be rejected")
    }

    @Test
    fun mergeRequiresNewData() {
        val outputDir = newTempDir("merge_no_data")
        val exitCode = debug(arrayOf("-t", "krill", "-q", "-D", outputDir.path, baseTar.path))
        assertNotEquals(0, exitCode, "Merging without any new input must be rejected")
    }
}
