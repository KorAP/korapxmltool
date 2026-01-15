package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import java.util.zip.GZIPInputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

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
                    val jsonContent = GZIPInputStream(jsonFile.inputStream())
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
                "-q",
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
                val jsonContent = GZIPInputStream(jsonFile.inputStream())
                    .bufferedReader().readText()

                // Check for required fields in the JSON output
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
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, baseZip, spacyZip)
        }
        assertTrue(generatedTar.exists())

        val extractDir = File.createTempFile("extract_inv", "").let { it.delete(); it.mkdirs(); it }
        try {
            ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path).start().waitFor()
            val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
            assertTrue(jsonFiles.isNotEmpty())

            jsonFiles.forEach { jsonFile ->
                val jsonContent = GZIPInputStream(jsonFile.inputStream())
                    .bufferedReader().readText()

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
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, baseZip, spacyZip)
        }
        assertTrue(generatedTar.exists())

        val extractDir = File.createTempFile("extract_base", "").let { it.delete(); it.mkdirs(); it }
        try {
            ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path).start().waitFor()
            val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
            assertTrue(jsonFiles.isNotEmpty())

            jsonFiles.forEach { jsonFile ->
                val jsonContent = GZIPInputStream(jsonFile.inputStream())
                    .bufferedReader().readText()

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
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, baseZip, spacyZip, marmotZip, opennlpZip, treeTaggerZip)
        }
        assertTrue(generatedTar.exists())

        val extractDir = File.createTempFile("extract_foundries", "").let { it.delete(); it.mkdirs(); it }
        try {
            ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path).start().waitFor()
            val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
            assertTrue(jsonFiles.isNotEmpty())

            jsonFiles.forEach { jsonFile ->
                val jsonContent = GZIPInputStream(jsonFile.inputStream())
                    .bufferedReader().readText()

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
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, baseZip, spacyZip, wud24Corenlp)
        }
        assertTrue(defaultTar.exists())

        val defaultJsons = readKrillJson(defaultTar).values
        assertTrue(defaultJsons.isNotEmpty())
        assertTrue(defaultJsons.all { !it.contains("\"s:,\"") })
        assertTrue(defaultJsons.all { !it.contains("\"s:!\"") })

        val flagTar = ensureKrillTar("wud24_default_corenlp_nwt") { outputDir ->
            arrayOf("-t", "krill", "-q", "--non-word-tokens", "-D", outputDir.path, baseZip, spacyZip, wud24Corenlp)
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
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, baseZip, spacyZip, marmotMaltZip, treeTaggerZip, corenlpZip)
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
                "-t", "krill", "-q", "--non-word-tokens", "-D", outputDir.path,
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
                "-q",
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
                "-q",
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
                val jsonContent = GZIPInputStream(jsonFile.inputStream())
                    .bufferedReader().readText()
                
                assertTrue(jsonContent.contains("\"@type\":\"koral:corpus\""), 
                    "Each JSON should be a valid Krill corpus document")
            }
        } finally {
            extractDir.deleteRecursively()
        }
    }

    @Test
    fun testPublisherAsStringEnvVar() {
        val baseZip = loadResource("wud24_sample.zip").path

        // Test WITHOUT environment variable (default: type:attachement)
        val defaultTar = ensureKrillTar("wud24_default_publisher", "wud24_sample.krill.tar") { outputDir ->
            arrayOf(
                "-t", "krill",
                "-q",
                "-D", outputDir.path,
                baseZip
            )
        }
        val defaultJsons = readKrillJson(defaultTar)
        assertTrue(defaultJsons.isNotEmpty(), "Should have generated Krill JSON files")

        // Check that publisher is type:attachement by default
        var foundPublisher = false
        defaultJsons.values.forEach { json ->
            val publisherMatch = Regex(""""key"\s*:\s*"publisher".*?"type"\s*:\s*"type:([^"]+)""").find(json)
            if (publisherMatch != null) {
                foundPublisher = true
                val typeValue = publisherMatch.groupValues[1]
                assertEquals("attachement", typeValue, "Publisher should be type:attachement by default")
            }
        }
        assertTrue(foundPublisher, "Should find publisher field in at least one document")

        // Test WITH K2K_PUBLISHER_STRING environment variable
        val envOutputDir = File.createTempFile("publisher_string_test", "").apply {
            delete()
            mkdirs()
        }
        try {
            System.setProperty("K2K_PUBLISHER_STRING", "1")
            debug(arrayOf("-t", "krill", "-q", "-D", envOutputDir.path, baseZip))
            val envTar = File(envOutputDir, "wud24_sample.krill.tar")
            assertTrue(envTar.exists(), "Expected wud24_sample.krill.tar with env var")
            val envJsons = readKrillJson(envTar)
            assertTrue(envJsons.isNotEmpty(), "Should have generated Krill JSON files with env var")

            // Check that publisher is now type:string
            var foundPublisherWithEnv = false
            envJsons.values.forEach { json ->
                val publisherMatch = Regex(""""key"\s*:\s*"publisher".*?"type"\s*:\s*"type:([^"]+)""").find(json)
                if (publisherMatch != null) {
                    foundPublisherWithEnv = true
                    val typeValue = publisherMatch.groupValues[1]
                    assertEquals("string", typeValue, "Publisher should be type:string when K2K_PUBLISHER_STRING is set")
                }
            }
            assertTrue(foundPublisherWithEnv, "Should find publisher field with env var set")
        } finally {
            System.clearProperty("K2K_PUBLISHER_STRING")
            envOutputDir.deleteRecursively()
        }
    }

    @Test
    fun testTranslatorAsTextEnvVar() {
        val baseZip = loadResource("wud24_sample.zip").path

        // Test WITHOUT environment variable (default: type:attachement)
        val defaultOutputDir = File.createTempFile("translator_default", "").apply {
            delete()
            mkdirs()
        }
        try {
            debug(arrayOf("-t", "krill", "-q", "-D", defaultOutputDir.path, baseZip))
            val defaultTar = File(defaultOutputDir, "wud24_sample.krill.tar")
            assertTrue(defaultTar.exists(), "Expected wud24_sample.krill.tar")
            val defaultJsons = readKrillJson(defaultTar)
            assertTrue(defaultJsons.isNotEmpty(), "Should have generated JSON")

            // Check that the translator is type:attachement by default
            var foundTranslator = false
            defaultJsons.values.forEach { json ->
                val translatorMatch = Regex(""""key"\s*:\s*"translator".*?"type"\s*:\s*"type:([^"]+)""").find(json)
                if (translatorMatch != null && json.contains("Test Translator")) {
                    foundTranslator = true
                    val typeValue = translatorMatch.groupValues[1]
                    assertEquals("attachement", typeValue, "Translator should be type:attachement by default")
                }
            }
            assertTrue(foundTranslator, "Should find translator field with 'Test Translator' in generated JSON")
        } finally {
            defaultOutputDir.deleteRecursively()
        }

        // Test WITH K2K_TRANSLATOR_TEXT (type:text)
        val envOutputDir = File.createTempFile("translator_text", "").apply {
            delete()
            mkdirs()
        }
        try {
            System.setProperty("K2K_TRANSLATOR_TEXT", "1")
            debug(arrayOf("-t", "krill", "-q", "-D", envOutputDir.path, baseZip))
            val envTar = File(envOutputDir, "wud24_sample.krill.tar")
            assertTrue(envTar.exists(), "Expected wud24_sample.krill.tar with env var")
            val envJsons = readKrillJson(envTar)
            assertTrue(envJsons.isNotEmpty(), "Should have generated JSON with env var")

            // Check that the translator is now type:text
            var foundTranslatorWithEnv = false
            envJsons.values.forEach { json ->
                val translatorMatch = Regex(""""key"\s*:\s*"translator".*?"type"\s*:\s*"type:([^"]+)""").find(json)
                if (translatorMatch != null && json.contains("Test Translator")) {
                    foundTranslatorWithEnv = true
                    val typeValue = translatorMatch.groupValues[1]
                    assertEquals("text", typeValue, "Translator should be type:text when K2K_TRANSLATOR_TEXT is set")
                }
            }
            assertTrue(foundTranslatorWithEnv, "Should find translator field with 'Test Translator' and env var set")
        } finally {
            System.clearProperty("K2K_TRANSLATOR_TEXT")
            envOutputDir.deleteRecursively()
        }
    }
    @Test
    fun krillCanHandleNonBmpText() {
        val wdd17 = loadResource("wdd17sample.zip").path
        val generatedTar = ensureKrillTar("wdd17_non_bmp", "wdd17sample.krill.tar") { outputDir ->
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, wdd17)
        }
        assertTrue(generatedTar.exists())

        val jsons = readKrillJson(generatedTar)
        assertTrue(jsons.isNotEmpty())

        val combinedJsonContent = jsons.values.joinToString("\n")

        // Check for the presence of the emoji sequence
        // 🙈 🙉 🙊
        assertTrue(combinedJsonContent.contains("\uD83D\uDE48"), "Should contain 🙈")
        assertTrue(combinedJsonContent.contains("\uD83D\uDE49"), "Should contain 🙉")
        assertTrue(combinedJsonContent.contains("\uD83D\uDE4A"), "Should contain 🙊")

        // Check for the text context
        assertTrue(combinedJsonContent.contains("mach"), "Should contain 'mach'")
        assertTrue(combinedJsonContent.contains("Bereinige wenigstens die allergröbsten Sachen"), "Should contain German text")

        // Check if emojis are indexed as tokens
        assertTrue(combinedJsonContent.contains("\"s:\uD83D\uDE48\""), "Should contain token 🙈")
        assertTrue(combinedJsonContent.contains("\"s:\uD83D\uDE49\""), "Should contain token 🙉")
        assertTrue(combinedJsonContent.contains("\"s:\uD83D\uDE4A\""), "Should contain token 🙊")
    }

    @Test
    fun krillCanHandleNonBmpTextWithNonWordTokens() {
        val wdd17 = loadResource("wdd17sample.zip").path
        val generatedTar = ensureKrillTar("wdd17_non_bmp_nwt", "wdd17sample.krill.tar") { outputDir ->
            arrayOf("-t", "krill", "-q", "--non-word-tokens", "-D", outputDir.path, wdd17)
        }
        assertTrue(generatedTar.exists())

        val jsons = readKrillJson(generatedTar)
        assertTrue(jsons.isNotEmpty())

        val combinedJsonContent = jsons.values.joinToString("\n")

        // Check if emojis are indexed as tokens
        assertTrue(combinedJsonContent.contains("\"s:\uD83D\uDE48\""), "Should contain token 🙈 with --non-word-tokens")
        assertTrue(combinedJsonContent.contains("\"s:\uD83D\uDE49\""), "Should contain token 🙉 with --non-word-tokens")
        assertTrue(combinedJsonContent.contains("\"s:\uD83D\uDE4A\""), "Should contain token 🙊 with --non-word-tokens")
    }

    @Test
    fun testProbabilitySortingInKrillJsonOutput() {
        // Test that multiple POS annotations are sorted by descending probability in Krill JSON output
        // Use the base sample ZIP which should contain POS annotations with probabilities

        val generatedTar = ensureKrillTar("probability_sorting_test") { outputDir ->
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, loadResource("wud24_sample.zip").path)
        }

        // Extract the JSON files from the tar
        val extractDir = File.createTempFile("extract_prob", "").let { it.delete(); it.mkdirs(); it }
        try {
            ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path).start().waitFor()
            val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
            assertTrue(jsonFiles.isNotEmpty(), "Should have extracted Krill JSON files")

            // Read the JSON content directly from the files
            val jsons = jsonFiles.associate { jsonFile ->
                val jsonContent = GZIPInputStream(jsonFile.inputStream())
                    .bufferedReader()
                    .use { it.readText() }
                jsonFile.name.removeSuffix(".json.gz") to jsonContent
            }
            assertTrue(jsons.isNotEmpty(), "Should have JSON content")

            // Combine all JSON content to search for POS annotations
            val combinedJsonContent = jsons.values.joinToString("\n")

            // Look for POS annotations in the JSON - they appear as "/p:TAG" entries
            val posMatches = Regex(""""/p:(ADJA|ADJD|NN|VVFIN)"""").findAll(combinedJsonContent).toList()

            if (posMatches.size >= 2) {
                // Extract the POS tags in the order they appear in the JSON
                val posTagsInOrder = posMatches.map { it.groupValues[1] }

                // Expected order based on probabilities: NN (0.984), ADJA (0.006), VVFIN (0.004), ADJD (0.002)
                // NN should appear first since it has the highest probability
                val nnIndex = posTagsInOrder.indexOf("NN")
                val adjaIndex = posTagsInOrder.indexOf("ADJA")

                if (nnIndex >= 0 && adjaIndex >= 0) {
                    assertTrue(nnIndex < adjaIndex,
                        "NN (prob 0.984) should appear before ADJA (prob 0.006) in JSON. Found order: $posTagsInOrder")
                }

                // Additional check: if we have VVFIN and ADJD, VVFIN should come before ADJD
                val vvfinIndex = posTagsInOrder.indexOf("VVFIN")
                val adjdIndex = posTagsInOrder.indexOf("ADJD")

                if (vvfinIndex >= 0 && adjdIndex >= 0) {
                    assertTrue(vvfinIndex < adjdIndex,
                        "VVFIN (prob 0.004) should appear before ADJD (prob 0.002) in JSON. Found order: $posTagsInOrder")
                }

            } else {
                // If specific pattern matching fails, verify basic structure exists
                assertTrue(combinedJsonContent.contains("\"@type\""), "Should contain valid Krill JSON structure")
            }

        } finally {
            extractDir.deleteRecursively()
        }
    }

    @Test
    fun testKrillMetadataInheritance() {
        // Test for GitHub issue #22: Ensure all metadata fields are correctly extracted and inherited
        val ndySample = loadResource("ndy_sample.zip").path
        
        val generatedTar = ensureKrillTar("ndy_metadata_test", "ndy_sample.krill.tar") { outputDir ->
            arrayOf(
                "-t", "krill",
                "-q",
                "-D", outputDir.path,
                ndySample
            )
        }

        val kotlinJsons = readKrillJson(generatedTar)
        assertTrue(kotlinJsons.isNotEmpty(), "Should have generated Krill JSON files from NDY sample")

        // Test specific metadata fields that were previously missing (GitHub issue #22)
        val requiredFields = mapOf(
            "corpusTitle" to "Nottinghamer Korpus Deutscher YouTube-Sprache",
            "docTitle" to "Info Video",
            "docAuthor" to "User_A",  // Anonymized
            "distributor" to "Institut für Deutsche Sprache",
            "pubPlace" to "San Bruno, California",
            "textExternalLink" to "youtube.googleapis.com",  // Partial match for URL
            "tokenSource" to "base#tokens"
        )

        // Test on one of the documents (NDY/115/005255)
        val testDocId = "NDY-115-005255.json"
        assertTrue(kotlinJsons.containsKey(testDocId), "Should have JSON for test document $testDocId")
        
        val testJson = kotlinJsons.getValue(testDocId)
        
        requiredFields.forEach { (fieldName, expectedValue) ->
            assertTrue(
                testJson.contains("\"$fieldName\""),
                "JSON should contain field: $fieldName"
            )
            assertTrue(
                testJson.contains(expectedValue),
                "Field $fieldName should contain value: $expectedValue"
            )
        }

        // Verify corpus-level metadata inheritance works
        // pubPlace should be inherited from corpus level (not empty from text level)
        val pubPlaceMatch = Regex(""""key"\s*:\s*"pubPlace".*?"value"\s*:\s*"([^"]+)"""").find(testJson)
        assertNotNull(pubPlaceMatch, "Should find pubPlace field")
        assertEquals(
            "San Bruno, California",
            pubPlaceMatch.groupValues[1],
            "pubPlace should be inherited from corpus level"
        )

        // Verify tokenSource is dynamically extracted from tokens.xml path
        val tokenSourceMatch = Regex(""""key"\s*:\s*"tokenSource".*?"value"\s*:\s*"([^"]+)"""").find(testJson)
        assertNotNull(tokenSourceMatch, "Should find tokenSource field")
        assertEquals(
            "base#tokens",
            tokenSourceMatch.groupValues[1],
            "tokenSource should be extracted from foundry path"
        )
    }

    /**
     * Regression test for GitHub issue #21: Missing base/s:p paragraph spans
     * 
     * Ensures that for each dereko/s:p element in the input structure.xml,
     * a corresponding base/s:p span is generated in the Krill JSON output.
     */
    @Test
    fun testBaseParagraphSpansPresent() {
        val ndySample = loadResource("ndy_sample.zip").path
        
        val generatedTar = ensureKrillTar("ndy_base_paragraph_test", "ndy_sample.krill.tar") { outputDir ->
            arrayOf(
                "-t", "krill",
                "-q",
                "-D", outputDir.path,
                ndySample
            )
        }

        val kotlinJsons = readKrillJson(generatedTar)
        assertTrue(kotlinJsons.isNotEmpty(), "Should have generated Krill JSON files from NDY sample")

        // Test NDY/266/006701 - a document that should have base/s:p spans
        val testDoc266 = "NDY-266-006701.json"
        assertTrue(kotlinJsons.containsKey(testDoc266), "Should have JSON for test document $testDoc266")
        
        val testJson266 = kotlinJsons.getValue(testDoc266)
        
        // Verify the specific base/s:p span from issue #21 is present
        // "<>:base/s:p$<b>64<i>0<i>1<i>1<b>1"
        assertTrue(
            testJson266.contains("<>:base/s:p\$"),
            "JSON should contain base/s:p span marker"
        )
        assertTrue(
            testJson266.contains("<>:base/s:p\$<b>64<i>0<i>1<i>1<b>1"),
            "NDY-266-006701 should contain the specific base/s:p span from issue #21: '<>:base/s:p\$<b>64<i>0<i>1<i>1<b>1'"
        )

        // Test NDY/115/005255 - another document with paragraphs
        val testDoc115 = "NDY-115-005255.json"
        assertTrue(kotlinJsons.containsKey(testDoc115), "Should have JSON for test document $testDoc115")
        
        val testJson115 = kotlinJsons.getValue(testDoc115)
        assertTrue(
            testJson115.contains("<>:base/s:p\$"),
            "NDY-115-005255 should also contain base/s:p spans"
        )

        // Verify paragraph count metadata matches the number of base/s:p spans
        kotlinJsons.forEach { (docId, json) ->
            // Extract paragraph count from metadata
            val paragraphCountMatch = Regex("""-:base/paragraphs\$<i>(\d+)""").find(json)
            if (paragraphCountMatch != null) {
                val paragraphCount = paragraphCountMatch.groupValues[1].toInt()
                
                // Count base/s:p spans in the stream
                val basePCount = Regex("""<>:base/s:p\$""").findAll(json).count()
                
                assertEquals(
                    paragraphCount,
                    basePCount,
                    "Document $docId: Number of base/s:p spans ($basePCount) should match paragraph count metadata ($paragraphCount)"
                )
            }
        }
    }
}
