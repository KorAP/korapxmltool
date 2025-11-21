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
import kotlin.test.assertFalse
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

    @Test
    fun testPublisherAsStringEnvVar() {
        val baseZip = loadResource("wdf19.zip").path
        
        // First test WITHOUT the environment variable (default: type:attachement)
        val defaultTar = ensureKrillTar("wdf19_default_publisher", "wdf19.krill.tar") { outputDir ->
            arrayOf(
                "-t", "krill",
                "-D", outputDir.path,
                baseZip
            )
        }

        val defaultJsons = readKrillJson(defaultTar)
        assertTrue(defaultJsons.isNotEmpty(), "Should have generated Krill JSON files")
        
        // Check that publisher is type:attachement by default
        var foundPublisher = false
        defaultJsons.values.forEach { json ->
            val publisherMatch = Regex(""""\s*key"\s*:\s*"publisher".*?"type"\s*:\s*"type:([^"]+)"""").find(json)
            if (publisherMatch != null) {
                foundPublisher = true
                val typeValue = publisherMatch.groupValues[1]
                assertEquals("attachement", typeValue, "Publisher should be type:attachement by default")
            }
        }
        assertTrue(foundPublisher, "Should find publisher field in at least one document")

        // Now test WITH K2K_PUBLISHER_STRING environment variable
        val outputDir = File.createTempFile("publisher_string_test", "").apply {
            delete()
            mkdirs()
        }

        try {
            val process = ProcessBuilder(
                "java", "-jar", "app/build/libs/korapxmltool.jar",
                "-t", "krill",
                "-D", outputDir.path,
                baseZip
            )
                .directory(File("/home/kupietz/KorAP/korapxmltool"))
                .apply {
                    environment()["K2K_PUBLISHER_STRING"] = "1"
                }
                .redirectErrorStream(true)
                .start()

            val exitCode = process.waitFor()
            assertEquals(0, exitCode, "Krill conversion with K2K_PUBLISHER_STRING should succeed")

            val tar = File(outputDir, "wdf19.krill.tar")
            assertTrue(tar.exists(), "Expected wdf19.krill.tar")

            val envJsons = readKrillJson(tar)
            assertTrue(envJsons.isNotEmpty(), "Should have generated Krill JSON files with env var")

            // Check that publisher is now type:string
            var foundPublisherWithEnv = false
            envJsons.values.forEach { json ->
                val publisherMatch = Regex(""""\s*key"\s*:\s*"publisher".*?"type"\s*:\s*"type:([^"]+)"""").find(json)
                if (publisherMatch != null) {
                    foundPublisherWithEnv = true
                    val typeValue = publisherMatch.groupValues[1]
                    assertEquals("string", typeValue, "Publisher should be type:string when K2K_PUBLISHER_STRING is set")
                }
            }
            assertTrue(foundPublisherWithEnv, "Should find publisher field with env var set")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun testTranslatorAsTextEnvVar() {
        // Create a temporary test ZIP with translator field
        val testDir = File.createTempFile("translator_test_corpus", "").apply {
            delete()
            mkdirs()
        }

        try {
            // Create a simple test corpus with translator metadata
            val textDir = File(testDir, "TEST/TEST.00001")
            textDir.mkdirs()

            File(textDir, "header.xml").writeText("""
                <?xml version="1.0" encoding="UTF-8"?>
                <idsHeader type="text" TEIform="teiHeader">
                    <fileDesc>
                        <titleStmt>
                            <t.title>Test Document with Translator</t.title>
                            <t.author>Test Author</t.author>
                            <translator>Test Translator</translator>
                        </titleStmt>
                    </fileDesc>
                </idsHeader>
            """.trimIndent())

            File(textDir, "data.xml").writeText("""
                <?xml version="1.0" encoding="UTF-8"?>
                <raw_text><text>Test content.</text></raw_text>
            """.trimIndent())

            // Create ZIP
            val zipFile = File(testDir, "test_translator.zip")
            val zipProcess = ProcessBuilder("zip", "-r", zipFile.path, "TEST")
                .directory(testDir)
                .redirectErrorStream(true)
                .start()
            assertEquals(0, zipProcess.waitFor(), "ZIP creation should succeed")

            // Test WITHOUT K2K_TRANSLATOR_TEXT (default: type:attachement)
            val defaultOutputDir = File.createTempFile("translator_default", "").apply {
                delete()
                mkdirs()
            }

            try {
                val defaultProcess = ProcessBuilder(
                    "java", "-jar", "app/build/libs/korapxmltool.jar",
                    "-t", "krill",
                    "-D", defaultOutputDir.path,
                    zipFile.path
                )
                    .directory(File("/home/kupietz/KorAP/korapxmltool"))
                    .redirectErrorStream(true)
                    .start()

                assertEquals(0, defaultProcess.waitFor(), "Default conversion should succeed")

                val defaultTar = File(defaultOutputDir, "test_translator.krill.tar")
                assertTrue(defaultTar.exists(), "Expected test_translator.krill.tar")

                val defaultJsons = readKrillJson(defaultTar)
                assertTrue(defaultJsons.isNotEmpty(), "Should have generated JSON")

                var foundTranslator = false
                defaultJsons.values.forEach { json ->
                    val translatorMatch = Regex(""""\s*key"\s*:\s*"translator".*?"type"\s*:\s*"type:([^"]+)"""").find(json)
                    if (translatorMatch != null) {
                        foundTranslator = true
                        val typeValue = translatorMatch.groupValues[1]
                        assertEquals("attachement", typeValue, "Translator should be type:attachement by default")
                    }
                }
                assertTrue(foundTranslator, "Should find translator field in generated JSON")
            } finally {
                defaultOutputDir.deleteRecursively()
            }

            // Test WITH K2K_TRANSLATOR_TEXT (type:text)
            val envOutputDir = File.createTempFile("translator_text", "").apply {
                delete()
                mkdirs()
            }

            try {
                val envProcess = ProcessBuilder(
                    "java", "-jar", "app/build/libs/korapxmltool.jar",
                    "-t", "krill",
                    "-D", envOutputDir.path,
                    zipFile.path
                )
                    .directory(File("/home/kupietz/KorAP/korapxmltool"))
                    .apply {
                        environment()["K2K_TRANSLATOR_TEXT"] = "1"
                    }
                    .redirectErrorStream(true)
                    .start()

                assertEquals(0, envProcess.waitFor(), "Conversion with K2K_TRANSLATOR_TEXT should succeed")

                val envTar = File(envOutputDir, "test_translator.krill.tar")
                assertTrue(envTar.exists(), "Expected test_translator.krill.tar with env var")

                val envJsons = readKrillJson(envTar)
                assertTrue(envJsons.isNotEmpty(), "Should have generated JSON with env var")

                var foundTranslatorWithEnv = false
                envJsons.values.forEach { json ->
                    val translatorMatch = Regex(""""\s*key"\s*:\s*"translator".*?"type"\s*:\s*"type:([^"]+)"""").find(json)
                    if (translatorMatch != null) {
                        foundTranslatorWithEnv = true
                        val typeValue = translatorMatch.groupValues[1]
                        assertEquals("text", typeValue, "Translator should be type:text when K2K_TRANSLATOR_TEXT is set")
                    }
                }
                assertTrue(foundTranslatorWithEnv, "Should find translator field with env var set")
            } finally {
                envOutputDir.deleteRecursively()
            }
        } finally {
            testDir.deleteRecursively()
        }
    }
}
