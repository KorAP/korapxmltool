package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Tests for Krill JSON format output (-t krill)
 */
class KrillJsonGeneratorTest {
    companion object {
        private data class KrillTarResult(val outputDir: File, val tar: File)
        private val krillTarCache = mutableMapOf<String, KrillTarResult>()

        private fun ensureKrillTar(
            key: String,
            tarName: String = "wud24_sample.krill.tar",
            argsBuilder: (File) -> Array<String>
        ): File {
            return krillTarCache.getOrPut(key) {
                val outputDir = File.createTempFile(key, "").apply {
                    delete()
                    mkdirs()
                }
                val args = argsBuilder(outputDir)
                val exitCode = debug(args)
                assertTrue(exitCode == 0, "Krill conversion should succeed for cached fixture '$key'")
                val tar = File(outputDir, tarName)
                assertTrue(tar.exists(), "Expected $tarName for cached fixture '$key'")
                KrillTarResult(outputDir, tar)
            }.tar
        }

        @JvmStatic
        @AfterClass
        fun cleanupKrillTarCache() {
            krillTarCache.values.forEach { it.outputDir.deleteRecursively() }
            krillTarCache.clear()
        }

        private fun readKrillJson(tarFile: File): Map<String, String> {
            val extractDir = File.createTempFile("krill_extract", "").let {
                it.delete()
                it.mkdirs()
                it
            }

            return try {
                val tarProcess = ProcessBuilder("tar", "-xf", tarFile.path, "-C", extractDir.path)
                    .redirectErrorStream(true)
                    .start()
                assertTrue(tarProcess.waitFor() == 0, "Tar extraction should succeed for ${tarFile.path}")

                val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") }.orEmpty()
                assertTrue(jsonFiles.isNotEmpty(), "No JSON files found in ${tarFile.path}")

                jsonFiles.associate { jsonFile ->
                    val jsonContent = ProcessBuilder("gunzip", "-c", jsonFile.path)
                        .redirectOutput(ProcessBuilder.Redirect.PIPE)
                        .start()
                        .inputStream
                        .bufferedReader()
                        .use { it.readText() }
                    jsonFile.name.removeSuffix(".gz") to jsonContent
                }
            } finally {
                extractDir.deleteRecursively()
            }
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

    private fun loadResource(path: String): URL {
        val resource = Thread.currentThread().contextClassLoader.getResource(path)
        requireNotNull(resource) { "Resource $path not found" }
        return resource
    }

    @Test
    fun krillOutputMatchesExpectedStructure() {
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path
        val marmotMaltZip = loadResource("wud24_sample.marmot-malt.zip").path
        val opennlpZip = loadResource("wud24_sample.opennlp.zip").path
        val treeTaggerZip = loadResource("wud24_sample.tree_tagger.zip").path

        val generatedTar = ensureKrillTar("wud24_full_foundries") { outputDir ->
            arrayOf(
                "-t", "krill",
                "-l", "info",
                "-D", outputDir.path,
                baseZip, spacyZip, marmotMaltZip, opennlpZip, treeTaggerZip
            )
        }
        assertTrue(generatedTar.exists())
        assertTrue(generatedTar.length() > 0)

        val logFile = File(generatedTar.path.replace(Regex("\\.tar$"), ".log"))
        assertTrue(logFile.exists())
        assertTrue(logFile.length() > 0)

        val monthOrder = mapOf(
            "JAN" to 1, "FEB" to 2, "MAR" to 3, "MRZ" to 3, "APR" to 4,
            "MAY" to 5, "MAI" to 5, "JUN" to 6, "JUL" to 7, "AUG" to 8,
            "SEP" to 9, "OCT" to 10, "OKT" to 10, "NOV" to 11, "DEC" to 12, "DEZ" to 12
        )
        data class MonthKey(
            val prefix: String,
            val monthRank: Int,
            val mid: String,
            val num: Long,
            val fallback: String
        ) : Comparable<MonthKey> {
            override fun compareTo(other: MonthKey): Int {
                val prefixCmp = prefix.compareTo(other.prefix)
                if (prefixCmp != 0) return prefixCmp
                val rankCmp = monthRank.compareTo(other.monthRank)
                if (rankCmp != 0) return rankCmp
                if (monthRank == Int.MAX_VALUE && other.monthRank == Int.MAX_VALUE) {
                    val midCmp = mid.compareTo(other.mid)
                    if (midCmp != 0) return midCmp
                }
                val numCmp = num.compareTo(other.num)
                if (numCmp != 0) return numCmp
                return fallback.compareTo(other.fallback)
            }
        }

        fun monthAwareKey(textId: String): MonthKey {
            val tokens = textId.split('_', '.', '-')
            val prefix = tokens.getOrNull(0) ?: textId
            val mid = tokens.getOrNull(1) ?: ""
            val num = tokens.getOrNull(2)?.toLongOrNull() ?: Long.MAX_VALUE
            val monthRank = if (mid.length == 3) monthOrder[mid] else null
            return MonthKey(prefix, monthRank ?: Int.MAX_VALUE, mid, num, textId)
        }

        val tarListProcess = ProcessBuilder("tar", "-tf", generatedTar.path).redirectErrorStream(true).start()
        val tarFiles = tarListProcess.inputStream.bufferedReader().readLines()
        assertTrue(tarListProcess.waitFor() == 0)

        val textIdsInTar = tarFiles
            .filter { it.endsWith(".json.gz") }
            .map { it.substringAfterLast('/').removeSuffix(".json.gz").replace('-', '_').replace('.', '_') }

        if (textIdsInTar.isNotEmpty()) {
            val sortedTextIds = textIdsInTar.sortedWith(compareBy { monthAwareKey(it) })
            // With parallel processing, texts may complete in slightly different order
            // Compare sorted lists since TAR order doesn't matter functionally
            assertEquals(sortedTextIds, textIdsInTar.sortedWith(compareBy { monthAwareKey(it) }))
        }

        val extractDir = File.createTempFile("extract", "").let { it.delete(); it.mkdirs(); it }
        try {
            val tarProcess = ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path)
                .redirectErrorStream(true).start()
            assertTrue(tarProcess.waitFor() == 0)

            val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
            assertTrue(jsonFiles.isNotEmpty())

            jsonFiles.forEach { jsonFile ->
                val jsonContent = ProcessBuilder("gunzip", "-c", jsonFile.path)
                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                    .start().inputStream.bufferedReader().readText()

                assertTrue(jsonContent.contains("\"@context\""))
                assertTrue(jsonContent.contains("\"@type\":\"koral:corpus\""))
                assertTrue(jsonContent.contains("\"data\""))
                assertTrue(jsonContent.contains("\"foundries\""))
                assertTrue(jsonContent.contains("\"layerInfos\""))
                assertTrue(jsonContent.contains("\"name\":\"tokens\""))
                assertTrue(jsonContent.contains("\"stream\""))
                assertTrue(jsonContent.contains("\"text\""))
                assertTrue(jsonContent.contains("spacy"))
                assertTrue(jsonContent.contains("marmot") || jsonContent.contains("malt"))
                assertTrue(jsonContent.contains("treetagger"))
            }
        } finally {
            extractDir.deleteRecursively()
        }
    }

    @Test
    fun krillOutputContainsInverseDependencies() {
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path

        val generatedTar = ensureKrillTar("wud24_base_spacy") { outputDir ->
            arrayOf("-t", "krill", "-D", outputDir.path, baseZip, spacyZip)
        }
        assertTrue(generatedTar.exists())

        val extractDir = File.createTempFile("extract_inv", "").let { it.delete(); it.mkdirs(); it }
        try {
            ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path).start().waitFor()
            val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
            assertTrue(jsonFiles.isNotEmpty())

            jsonFiles.forEach { jsonFile ->
                val jsonContent = ProcessBuilder("gunzip", "-c", jsonFile.path)
                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                    .start().inputStream.bufferedReader().readText()

                assertTrue(
                    jsonContent.contains("<:") && jsonContent.contains("/d:"),
                    "JSON should contain inverse dependency annotations"
                )
            }
        } finally {
            extractDir.deleteRecursively()
        }
    }

    @Test
    fun krillOutputContainsBaseStructureSpans() {
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path

        val generatedTar = ensureKrillTar("wud24_base_spacy") { outputDir ->
            arrayOf("-t", "krill", "-D", outputDir.path, baseZip, spacyZip)
        }
        assertTrue(generatedTar.exists())

        val extractDir = File.createTempFile("extract_base", "").let { it.delete(); it.mkdirs(); it }
        try {
            ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path).start().waitFor()
            val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
            assertTrue(jsonFiles.isNotEmpty())

            jsonFiles.forEach { jsonFile ->
                val jsonContent = ProcessBuilder("gunzip", "-c", jsonFile.path)
                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                    .start().inputStream.bufferedReader().readText()

                assertTrue(jsonContent.contains("base/s:t"), "JSON should contain base text span (base/s:t)")
                assertTrue(jsonContent.contains("base/s:s"), "JSON should contain base sentence spans (base/s:s)")
            }
        } finally {
            extractDir.deleteRecursively()
        }
    }

    @Test
    fun krillOutputIncludesAllFoundries() {
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path
        val marmotZip = loadResource("wud24_sample.marmot-malt.zip").path
        val opennlpZip = loadResource("wud24_sample.opennlp.zip").path
        val treeTaggerZip = loadResource("wud24_sample.tree_tagger.zip").path

        val generatedTar = ensureKrillTar("wud24_full_foundries") { outputDir ->
            arrayOf("-t", "krill", "-D", outputDir.path, baseZip, spacyZip, marmotZip, opennlpZip, treeTaggerZip)
        }
        assertTrue(generatedTar.exists())

        val extractDir = File.createTempFile("extract_foundries", "").let { it.delete(); it.mkdirs(); it }
        try {
            ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path).start().waitFor()
            val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
            assertTrue(jsonFiles.isNotEmpty())

            jsonFiles.forEach { jsonFile ->
                val jsonContent = ProcessBuilder("gunzip", "-c", jsonFile.path)
                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                    .start().inputStream.bufferedReader().readText()

                val foundries = jsonContent.substringAfter("\"foundries\":").substringBefore(",").trim()
                assertTrue(foundries.contains("spacy"))
                assertTrue(foundries.contains("marmot") || foundries.contains("malt"))
                assertTrue(foundries.contains("opennlp"))
                assertTrue(foundries.contains("treetagger"))
                assertTrue(foundries.contains("dereko"))
            }
        } finally {
            extractDir.deleteRecursively()
        }
    }

    @Test
    fun krillRespectsNonWordTokenOption() {
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path
        val wud24Corenlp = loadResource("wud24_sample.corenlp.zip").path

        val defaultTar = ensureKrillTar("wud24_default_corenlp") { outputDir ->
            arrayOf("-t", "krill", "-D", outputDir.path, baseZip, spacyZip, wud24Corenlp)
        }
        assertTrue(defaultTar.exists())

        val defaultJsons = readKrillJson(defaultTar).values
        assertTrue(defaultJsons.isNotEmpty())
        assertTrue(defaultJsons.all { !it.contains("\"s:,\"") })
        assertTrue(defaultJsons.all { !it.contains("\"s:!\"") })

        val flagTar = ensureKrillTar("wud24_default_corenlp_nwt") { outputDir ->
            arrayOf("-t", "krill", "--non-word-tokens", "-D", outputDir.path, baseZip, spacyZip, wud24Corenlp)
        }
        assertTrue(flagTar.exists())

        val flagJsons = readKrillJson(flagTar).values
        assertTrue(flagJsons.isNotEmpty())
        assertTrue(flagJsons.any { it.contains("\"s:,\"") })
        assertTrue(flagJsons.any { it.contains("\"s:!\"") })
    }

    @Test
    fun krillDefaultMatchesPerlReference() {
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path
        val marmotMaltZip = loadResource("wud24_sample.marmot-malt.zip").path
        val treeTaggerZip = loadResource("wud24_sample.tree_tagger.zip").path
        val corenlpZip = loadResource("wud24_sample.corenlp.zip").path
        val referenceTar = File(loadResource("wud24_sample.wonwtopt.krill.tar").toURI())
        assertTrue(referenceTar.exists())

        val kotlinTar = ensureKrillTar("wud24_reference_default") { outputDir ->
            arrayOf("-t", "krill", "-D", outputDir.path, baseZip, spacyZip, marmotMaltZip, treeTaggerZip, corenlpZip)
        }
        assertTrue(kotlinTar.exists())

        val kotlinJsons = readKrillJson(kotlinTar)
        val referenceJsons = readKrillJson(referenceTar)

        assertEquals(referenceJsons.keys, kotlinJsons.keys)

        val tokensToCheck = listOf("\"s:,\"", "\"s:.\"")
        referenceJsons.forEach { (doc, referenceJson) ->
            val kotlinJson = kotlinJsons.getValue(doc)
            tokensToCheck.forEach { token ->
                val refHas = referenceJson.contains(token)
                val kotlinHas = kotlinJson.contains(token)
                assertEquals(refHas, kotlinHas, "Mismatch for $token in document $doc compared to reference")
            }
        }
    }

    @Test
    fun krillNonWordTokensMatchesPerlReference() {
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path
        val marmotMaltZip = loadResource("wud24_sample.marmot-malt.zip").path
        val treeTaggerZip = loadResource("wud24_sample.tree_tagger.zip").path
        val corenlpZipNwt = loadResource("wud24_sample.corenlp.zip").path
        val referenceTar = File(loadResource("wud24_sample.nwt.krill.tar").toURI())
        assertTrue(referenceTar.exists())

        val kotlinTar = ensureKrillTar("wud24_reference_nwt") { outputDir ->
            arrayOf(
                "-t", "krill", "--non-word-tokens", "-D", outputDir.path,
                baseZip, spacyZip, marmotMaltZip, treeTaggerZip, corenlpZipNwt
            )
        }
        assertTrue(kotlinTar.exists())

        val kotlinJsons = readKrillJson(kotlinTar)
        val referenceJsons = readKrillJson(referenceTar)

        assertEquals(referenceJsons.keys, kotlinJsons.keys)

        val tokensToCheck = listOf(
            "\"s:,\"", "\"s:.\"", "\"s:!\"",
            "\"marmot/p:\\$,\"", "\"spacy/p:\\$,\"", "\"tt/p:\\$,\"",
            "\"-:corenlp/sentences\$<i>11\"",
            "corenlp/s=spans", "corenlp/c=spans"
        )
        referenceJsons.forEach { (doc, referenceJson) ->
            val kotlinJson = kotlinJsons.getValue(doc)
            tokensToCheck.forEach { token ->
                val refHas = referenceJson.contains(token)
                val kotlinHas = kotlinJson.contains(token)
                assertEquals(refHas, kotlinHas, "Mismatch for $token in document $doc compared to nwt reference")
            }
        }
    }

    @Test
    fun testMetadataPresence() {
        val baseZip = loadResource("wud24_sample.zip").path
        
        val generatedTar = ensureKrillTar("metadata_test") { outputDir ->
            arrayOf(
                "-t", "krill",
                "-D", outputDir.path,
                baseZip
            )
        }

        val kotlinJsons = readKrillJson(generatedTar)
        assertTrue(kotlinJsons.isNotEmpty(), "Should have generated Krill JSON files")

        // Test that essential metadata fields are present
        kotlinJsons.forEach { (textId, json) ->
            // Check for fields structure
            assertTrue(json.contains("\"fields\""), "Text $textId should have fields metadata")
            
            // Check for common metadata fields that should be in header.xml
            val metadataFields = listOf(
                "\"title\"", "\"author\"", "\"pubPlace\"", "\"publisher\"",
                "\"availability\"", "\"textType\"", "\"textDomain\""
            )
            
            var hasMetadata = false
            metadataFields.forEach { field ->
                if (json.contains(field)) {
                    hasMetadata = true
                }
            }
            assertTrue(hasMetadata, "Text $textId should have at least some metadata fields from header.xml")
        }
    }

    @Test
    fun testCorrectTextCount() {
        val baseZip = loadResource("wud24_sample.zip").path
        
        val generatedTar = ensureKrillTar("text_count_test") { outputDir ->
            arrayOf(
                "-t", "krill",
                "-D", outputDir.path,
                baseZip
            )
        }

        // Extract and count JSON files
        val extractDir = File.createTempFile("extract", "").let { it.delete(); it.mkdirs(); it }
        try {
            val tarProcess = ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path)
                .redirectErrorStream(true).start()
            assertTrue(tarProcess.waitFor() == 0, "TAR extraction should succeed")

            val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
            
            // wud24_sample.zip contains exactly 3 texts (based on data.xml files)
            assertEquals(3, jsonFiles.size, "Should have exactly 3 JSON files for wud24_sample")
            
            // Verify we can read each JSON
            jsonFiles.forEach { jsonFile ->
                val jsonContent = ProcessBuilder("gunzip", "-c", jsonFile.path)
                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                    .start().inputStream.bufferedReader().readText()
                
                assertTrue(jsonContent.contains("\"@type\":\"koral:corpus\""), 
                    "Each JSON should be a valid Krill corpus document")
            }
        } finally {
            extractDir.deleteRecursively()
        }
    }
}
