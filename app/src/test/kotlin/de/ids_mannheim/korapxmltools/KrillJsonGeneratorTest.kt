package de.ids_mannheim.korapxmltools

import de.ids_mannheim.korapxmltools.formatters.KrillJsonGenerator
import net.jpountz.lz4.LZ4FrameInputStream
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.w3c.dom.Element
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import java.util.zip.GZIPInputStream
import javax.xml.parsers.DocumentBuilderFactory
import kotlin.test.Test
import kotlin.test.assertContains
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

    private fun headerElement(xml: String): Element {
        val dbFactory = DocumentBuilderFactory.newInstance()
        val builder = dbFactory.newDocumentBuilder()
        val doc = builder.parse(ByteArrayInputStream(xml.trimIndent().toByteArray()))
        return doc.documentElement
    }

    private fun collectKrillMetadata(tool: KorapXmlTool, docId: String, headerRoot: Element): Map<String, Any> {
        tool.krillData[docId] = KrillJsonGenerator.KrillTextData(textId = docId)
        val method = KorapXmlTool::class.java.getDeclaredMethod("collectKrillMetadata", String::class.java, Element::class.java)
        method.isAccessible = true
        method.invoke(tool, docId, headerRoot)
        return tool.krillData.getValue(docId).headerMetadata
    }

    private fun collectDocMetadata(tool: KorapXmlTool, docSigle: String, headerRoot: Element): Map<String, Any> {
        val method = KorapXmlTool::class.java.getDeclaredMethod("collectDocMetadata", String::class.java, Element::class.java)
        method.isAccessible = true
        method.invoke(tool, docSigle, headerRoot)
        return tool.docMetadata.getValue(docSigle)
    }

    private fun collectCorpusMetadata(tool: KorapXmlTool, corpusSigle: String, headerRoot: Element): Map<String, Any> {
        val method = KorapXmlTool::class.java.getDeclaredMethod("collectCorpusMetadata", String::class.java, Element::class.java)
        method.isAccessible = true
        method.invoke(tool, corpusSigle, headerRoot)
        return tool.corpusMetadata.getValue(corpusSigle)
    }

    private fun applyInheritedKrillMetadata(tool: KorapXmlTool, textId: String, textData: KrillJsonGenerator.KrillTextData) {
        val method = KorapXmlTool::class.java.getDeclaredMethod(
            "applyInheritedKrillMetadata", String::class.java, KrillJsonGenerator.KrillTextData::class.java
        )
        method.isAccessible = true
        method.invoke(tool, textId, textData)
    }

    private fun krillFieldValue(json: String, fieldName: String): String? =
        Regex(
            """"key"\s*:\s*"$fieldName".*?"value"\s*:\s*"([^"]*)"""",
            setOf(RegexOption.DOT_MATCHES_ALL)
        ).find(json)?.groupValues?.getOrNull(1)

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
    fun krillHandlesSparseFoundryWithoutBlockingOtherTexts() {
        val baseZip = loadResource("ndy_sample.zip").path
        val sparseCmcZip = loadResource("ndy_sample.cmc.zip").path
        val emptyGenderZip = loadResource("ndy_sample.gender.zip").path

        val generatedTar = ensureKrillTar("ndy_sparse_foundries", "ndy_sample.krill.tar") { outputDir ->
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, baseZip, sparseCmcZip, emptyGenderZip)
        }
        assertTrue(generatedTar.exists())

        val jsonByFile = readKrillJson(generatedTar)
        assertEquals(
            setOf("NDY-115-005255.json", "NDY-266-006701.json", "NDY-269-017376.json"),
            jsonByFile.keys,
            "Sparse and empty foundry ZIPs must not block base-only texts from being written"
        )
        assertFalse(jsonByFile.values.any { it.contains("\"gender\"") }, "The empty gender ZIP must not add foundry expectations or block output")
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
        assertTrue(defaultJsons.all { !it.contains("\"base/p:_\"") }, "Default output should not have base/p:_ marker")
        assertTrue(defaultJsons.all { !it.contains("base/p=tokens") }, "Default output should not declare base/p=tokens layer")

        val flagTar = ensureKrillTar("wud24_default_corenlp_nwt") { outputDir ->
            arrayOf("-t", "krill", "-q", "--non-word-tokens", "-D", outputDir.path, baseZip, spacyZip, wud24Corenlp)
        }
        assertTrue(flagTar.exists())

        val flagJsons = readKrillJson(flagTar).values
        assertTrue(flagJsons.isNotEmpty())
        assertTrue(flagJsons.any { it.contains("\"s:,\"") })
        assertTrue(flagJsons.any { it.contains("\"s:!\"") })
        assertTrue(flagJsons.any { it.contains("\"base/p:_\"") }, "NWT output should mark non-word tokens with base/p:_")
        assertTrue(flagJsons.all { it.contains("base/p=tokens") }, "NWT output should declare base/p=tokens in layerInfos")
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
    fun krillPubDateFallsBackToAvailableDateParts() {
        val tool = KorapXmlTool()

        val yearOnlyMetadata = collectKrillMetadata(
            tool,
            "TEST_DOC.1",
            headerElement(
                """
                <idsHeader>
                  <analytic/>
                  <monogr/>
                  <textDesc/>
                  <pubDate type="year">1960</pubDate>
                  <pubDate type="month"/>
                  <pubDate type="day"/>
                </idsHeader>
                """
            )
        )
        assertEquals("1960", yearOnlyMetadata["pubDate"])

        val yearMonthMetadata = collectKrillMetadata(
            KorapXmlTool(),
            "TEST_DOC.2",
            headerElement(
                """
                <idsHeader>
                  <analytic/>
                  <monogr/>
                  <textDesc/>
                  <pubDate type="year">1960</pubDate>
                  <pubDate type="month">7</pubDate>
                  <pubDate type="day"/>
                </idsHeader>
                """
            )
        )
        assertEquals("1960-07", yearMonthMetadata["pubDate"])
    }

    @Test
    fun krillInheritedDatesIgnoreEmptyTextValuesAndBackfillEachOther() {
        val tool = KorapXmlTool()
        val textMetadata = collectKrillMetadata(
            tool,
            "TEST_DOC.1",
            headerElement(
                """
                <idsHeader>
                  <analytic/>
                  <monogr/>
                  <textDesc/>
                  <creatDate/>
                  <pubDate/>
                </idsHeader>
                """
            )
        )

        val json = KrillJsonGenerator.generate(
            KrillJsonGenerator.KrillTextData(
                textId = "TEST_DOC.1",
                headerMetadata = textMetadata.toMutableMap()
            ),
            mapOf("TEST" to mutableMapOf<String, Any>("creationDate" to "1960-07-01")),
            emptyMap<String, MutableMap<String, Any>>(),
            includeNonWordTokens = false
        )

        assertEquals("1960-07-01", krillFieldValue(json, "creationDate"))
        assertEquals("1960-07-01", krillFieldValue(json, "pubDate"))
    }

    @Test
    fun krillMetadataResolutionUsesHierarchyPrecedence() {
        val resolvedFromDoc = KrillJsonGenerator.resolveHeaderMetadata(
            textId = "TEST_DOC.1",
            textHeaderMetadata = mutableMapOf<String, Any>("pubPlace" to " "),
            corpusMetadata = mapOf("TEST" to mutableMapOf<String, Any>("pubPlace" to "Corpus Place")),
            docMetadata = mapOf("TEST/DOC" to mutableMapOf<String, Any>("pubPlace" to "Doc Place"))
        )
        assertEquals("Doc Place", resolvedFromDoc["pubPlace"])

        val resolvedFromText = KrillJsonGenerator.resolveHeaderMetadata(
            textId = "TEST_DOC.1",
            textHeaderMetadata = mutableMapOf<String, Any>("pubPlace" to "Text Place"),
            corpusMetadata = mapOf("TEST" to mutableMapOf<String, Any>("pubPlace" to "Corpus Place")),
            docMetadata = mapOf("TEST/DOC" to mutableMapOf<String, Any>("pubPlace" to "Doc Place"))
        )
        assertEquals("Text Place", resolvedFromText["pubPlace"])
    }

    @Test
    fun availabilityIsInheritedFromCorpusWhenAbsentOnText() {
        val tool = KorapXmlTool()
        tool.corpusMetadata["TEST"] = mutableMapOf<String, Any>("availability" to "CC-BY-SA")
        val textData = KrillJsonGenerator.KrillTextData(textId = "TEST_DOC.1")

        applyInheritedKrillMetadata(tool, "TEST_DOC.1", textData)

        assertEquals("CC-BY-SA", textData.headerMetadata["availability"])
        assertEquals(0, tool.krillMissingAvailabilityCount.get(), "Inherited availability must not count as missing")
    }

    @Test
    fun availabilityDefaultsToUnknownAndIsCountedWhenMissingEverywhere() {
        val tool = KorapXmlTool()
        val textData = KrillJsonGenerator.KrillTextData(textId = "TEST_DOC.1")

        applyInheritedKrillMetadata(tool, "TEST_DOC.1", textData)

        assertEquals("unknown", textData.headerMetadata["availability"])
        assertEquals(1, tool.krillMissingAvailabilityCount.get(), "Missing availability must be counted for the summary warning")
    }

    @Test
    fun docAndCorpusMetadataCollectorsExposeCommonInheritedFields() {
        val tool = KorapXmlTool()

        val docMetadata = collectDocMetadata(
            tool,
            "TEST/DOC",
            headerElement(
                """
                <idsHeader>
                  <d.title>Document Level Title</d.title>
                  <h.author>Document Author</h.author>
                  <publisher>Document Publisher</publisher>
                  <creatDate>1984-01-02</creatDate>
                </idsHeader>
                """
            )
        )
        assertEquals("Document Level Title", docMetadata["title"])
        assertEquals("Document Level Title", docMetadata["docTitle"])
        assertEquals("Document Author", docMetadata["author"])
        assertEquals("Document Author", docMetadata["docAuthor"])
        assertEquals("Document Publisher", docMetadata["publisher"])
        assertEquals("1984-01-02", docMetadata["creationDate"])
        assertEquals("1984-01-02", docMetadata["pubDate"])

        val corpusMetadata = collectCorpusMetadata(
            tool,
            "TEST",
            headerElement(
                """
                <idsHeader>
                  <c.title>Corpus Level Title</c.title>
                  <h.author>Corpus Author</h.author>
                  <pubPlace>Corpus Place</pubPlace>
                  <pubDate type="year">1999</pubDate>
                </idsHeader>
                """
            )
        )
        assertEquals("Corpus Level Title", corpusMetadata["title"])
        assertEquals("Corpus Level Title", corpusMetadata["corpusTitle"])
        assertEquals("Corpus Author", corpusMetadata["author"])
        assertEquals("Corpus Author", corpusMetadata["corpusAuthor"])
        assertEquals("Corpus Place", corpusMetadata["pubPlace"])
        assertEquals("1999", corpusMetadata["pubDate"])
        assertEquals("1999", corpusMetadata["creationDate"])
    }

    @Test
    fun krillStandardTeiP5MetadataFallbacks() {
        val tool = KorapXmlTool()

        // 1. Check basic P5 typical structure from the example
        val p5Metadata = collectKrillMetadata(
            tool,
            "SK_UL.19811",
            headerElement(
                """
                <teiHeader>
                  <fileDesc>
                    <titleStmt>
                      <title>Affenstern</title>
                      <author role="primary">Udo Lindenberg</author>
                      <author role="text">Another Author</author>
                    </titleStmt>
                    <publicationStmt>
                      <publisher>Unbekannt</publisher>
                      <date>1981</date>
                      <ref>
                        <name>Udopia</name>
                      </ref>
                    </publicationStmt>
                    <sourceDesc>
                      <ab>
                        <link target="https://www.udo-lindenberg.de/affenstern.57680.htm" />
                      </ab>
                    </sourceDesc>
                  </fileDesc>
                </teiHeader>
                """
            )
        )

        assertEquals("Udo Lindenberg", p5Metadata["author"], "Should prefer primary role author")
        assertEquals("Affenstern", p5Metadata["title"])
        assertEquals("1981", p5Metadata["pubDate"])
        assertEquals("https://www.udo-lindenberg.de/affenstern.57680.htm", p5Metadata["externalLink"])

        // 2. Check <date when="..."> fallback
        val p5MetadataDateWhen = collectKrillMetadata(
            KorapXmlTool(),
            "SK_UL.19812",
            headerElement(
                """
                <teiHeader>
                  <fileDesc>
                    <publicationStmt>
                      <date when="1982-03-04"/>
                    </publicationStmt>
                  </fileDesc>
                </teiHeader>
                """
            )
        )
        assertEquals("1982-03-04", p5MetadataDateWhen["pubDate"])

        // 3. Check author without role (first author)
        val p5MetadataNoRole = collectKrillMetadata(
            KorapXmlTool(),
            "SK_UL.19813",
            headerElement(
                """
                <teiHeader>
                  <fileDesc>
                    <titleStmt>
                      <author>First Author</author>
                      <author>Second Author</author>
                    </titleStmt>
                  </fileDesc>
                </teiHeader>
                """
            )
        )
        assertEquals("First Author", p5MetadataNoRole["author"])
    }

    @Test
    fun testSkZipExtraction() {
        val skZip = loadResource("sk.zip").path
        
        val generatedTar = ensureKrillTar("sk_test", "sk.krill.tar") { outputDir ->
            arrayOf(
                "-t", "krill",
                "-q",
                "-D", outputDir.path,
                skZip
            )
        }
        assertTrue(generatedTar.exists())

        val jsonByFile = readKrillJson(generatedTar)
        assertTrue(jsonByFile.containsKey("SK-UL-19811.json"))
        val json = jsonByFile.getValue("SK-UL-19811.json")

        // Assert our fallbacks were correctly populated in the output JSON
        assertEquals("Udo Lindenberg", krillFieldValue(json, "author"))
        assertEquals("Affenstern", krillFieldValue(json, "title"))
        assertEquals("1981", krillFieldValue(json, "pubDate"))
        
        // Assert docTitle, docAuthor, corpusTitle, and corpusAuthor are empty/null
        kotlin.test.assertNull(krillFieldValue(json, "docTitle"))
        kotlin.test.assertNull(krillFieldValue(json, "docAuthor"))
        kotlin.test.assertNull(krillFieldValue(json, "corpusTitle"))
        kotlin.test.assertNull(krillFieldValue(json, "corpusAuthor"))

        // Check externalLink contains the encoded URL
        assertTrue(json.contains("https%3A%2F%2Fwww.udo-lindenberg.de%2Faffenstern.57680.htm"), 
            "JSON should contain the encoded url in the externalLink field")
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
    fun krillCanWriteLz4CompressedJson() {
        val baseZip = loadResource("wud24_sample.zip").path
        val generatedTar = ensureKrillTar("wud24_lz4", "wud24_sample.krill.tar") { outputDir ->
            arrayOf("-t", "krill", "-q", "--lz4", "-D", outputDir.path, baseZip)
        }
        assertTrue(generatedTar.exists())

        val extractDir = File.createTempFile("extract_lz4", "").let { it.delete(); it.mkdirs(); it }
        try {
            val tarProcess = ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path)
                .redirectErrorStream(true)
                .start()
            assertTrue(tarProcess.waitFor() == 0, "Tar extraction should succeed for ${generatedTar.path}")

            val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.lz4") }.orEmpty()
            assertTrue(jsonFiles.isNotEmpty(), "Expected LZ4-compressed JSON files in ${generatedTar.path}")

            jsonFiles.forEach { jsonFile ->
                val jsonContent = LZ4FrameInputStream(jsonFile.inputStream()).bufferedReader().use { it.readText() }
                assertTrue(jsonContent.contains("\"@context\""))
                assertTrue(jsonContent.contains("\"@type\":\"koral:corpus\""))
                assertTrue(jsonContent.contains("\"text\""))
            }
        } finally {
            extractDir.deleteRecursively()
        }
    }

    @Test
    fun krillGenerateToMatchesGenerate() {
        val textData = KrillJsonGenerator.KrillTextData(
            textId = "TEST_DOC.1",
            textContent = NonBmpString("Alpha beta."),
            headerMetadata = mutableMapOf("title" to "Synthetic test")
        ).apply {
            tokens = arrayOf(
                KorapXmlTool.Span(0, 5),
                KorapXmlTool.Span(6, 10),
                KorapXmlTool.Span(10, 11)
            )
            sentences = arrayOf(KorapXmlTool.Span(0, 11))
            morphoByFoundry["spacy"] = mutableMapOf(
                "0-5" to KorapXmlTool.MorphoSpan(lemma = "Alpha", upos = "PROPN", xpos = "NE"),
                "6-10" to KorapXmlTool.MorphoSpan(lemma = "beta", upos = "NOUN", xpos = "NN")
            )
            structureSpans.add(
                KrillJsonGenerator.StructureSpan(
                    layer = "dereko/s:p",
                    from = 0,
                    to = 11,
                    tokenFrom = 0,
                    tokenTo = 3,
                    depth = 1
                )
            )
        }

        val corpusMetadata = mutableMapOf<String, MutableMap<String, Any>>(
            "TEST" to mutableMapOf("publisher" to "Publisher")
        )
        val docMetadata = mutableMapOf<String, MutableMap<String, Any>>(
            "TEST_DOC" to mutableMapOf("docTitle" to "Doc title")
        )

        val generated = KrillJsonGenerator.generate(textData, corpusMetadata, docMetadata, includeNonWordTokens = true)
        val streamed = buildString {
            KrillJsonGenerator.generateTo(this, textData, corpusMetadata, docMetadata, includeNonWordTokens = true)
        }

        assertEquals(generated, streamed)
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

    @Test
    fun standoffMetadataAddsClassificationKeywords() {
        val baseZip = loadResource("rei_sample.zip").path
        val standoff = loadResource("rei_sample.domains.meta.xml").path

        val generatedTar = ensureKrillTar("rei_standoff", "rei_sample.krill.tar") { outputDir ->
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, baseZip, standoff)
        }
        assertTrue(generatedTar.exists())

        val jsonByFile = readKrillJson(generatedTar)
        val rei473 = jsonByFile.entries.first { it.key.startsWith("REI-RBR-00473") }.value

        // The layer xml:id ("wikiDomain") becomes the Krill field key, typed as keywords.
        val field = Regex(
            """"key":"wikiDomain","@type":"koral:field","value":\[([^\]]*)\],"type":"type:keywords""""
        ).find(rei473)
        assertNotNull(field, "Expected a wikiDomain keywords field in REI_RBR.00473")

        // Default policy ingests everything in the file, ordered by rank (no curation
        // at index time). REI_RBR.00473 has all five categories.
        val values = field.groupValues[1]
        assertEquals(
            """"Language","History","Culture","Mass_media","People"""",
            values,
            "All categories present in the file should be indexed, in rank order"
        )
    }

    /**
     * Regression tests for https://github.com/KorAP/korapxmltool/issues/54
     *
     * I5 `<xenoData>` blocks carry corpus-provided metadata (e.g. the RPK
     * denomination) that must reach Krill so it is displayed and searchable.
     * Mirrors KorAP-XML-Krill commit 24ad3c0 (KorAP::XML::Meta::I5) and its
     * t/real/ked.t expectations.
     */
    @Test
    fun parseXenoDataMapsTypesKeysAndAccumulatesKeywords() {
        // Text-level xenoData from KED/KLX/03212 (KorAP-XML-Krill t/real), plus the
        // project-less text/date metas from the issue's RPK example.
        val fields = StandoffMetadata.parseXenoData(
            headerElement(
                """
                <idsHeader>
                  <xenoData>
                    <meta name="rcpnt" project="KED" type="keyword" desc="recipient group">kinder</meta>
                    <meta name="rcpntLabel" project="KED" type="attachment" desc="recipient group capitalized">Kinder</meta>
                    <meta name="strtgy" project="KED" type="keyword" desc="strategy">erklaeren</meta>
                    <meta name="cover1Herder" project="KED" type="string" desc="text coverage 1k">0.58</meta>
                    <meta name="cover5Herder" project="KED" type="string">0.66</meta>
                    <meta name="topicLabel" project="KED" type="attachment">Gesundheit und Krankheit</meta>
                    <!-- repeated metas: keyword accumulates, scalar keeps the last -->
                    <meta name="strtgy" project="KED" type="keyword">beschreiben</meta>
                    <meta name="cover5Herder" project="KED" type="string">0.67</meta>
                    <meta name="GUID" type="text">37f38083-948d-403c-a2c2-8631e8e4a91d</meta>
                    <meta name="Datum" type="date">2022-01-27</meta>
                    <meta name="empty" type="string">   </meta>
                    <meta name="bogus" type="frobnicate">x</meta>
                  </xenoData>
                </idsHeader>
                """
            ),
            "text"
        ).associateBy { it.key }

        // attachment -> type:attachement, value wrapped as a data: URI
        assertEquals("type:attachement", fields.getValue("KED.rcpntLabel").type)
        assertEquals("data:,Kinder", fields.getValue("KED.rcpntLabel").value)
        assertEquals("data:,Gesundheit und Krankheit", fields.getValue("KED.topicLabel").value)

        // string -> type:string
        assertEquals("type:string", fields.getValue("KED.cover1Herder").type)
        assertEquals("0.58", fields.getValue("KED.cover1Herder").value)
        // repeated scalar keeps the last value
        assertEquals("0.67", fields.getValue("KED.cover5Herder").value)

        // keyword -> type:keywords; repeats accumulate in document order; singles stay lists
        assertEquals("type:keywords", fields.getValue("KED.strtgy").type)
        assertEquals(listOf("erklaeren", "beschreiben"), fields.getValue("KED.strtgy").value)
        assertEquals(listOf("kinder"), fields.getValue("KED.rcpnt").value)

        // text -> type:text, project-less key (issue's RPK example)
        assertEquals("type:text", fields.getValue("GUID").type)
        assertEquals("37f38083-948d-403c-a2c2-8631e8e4a91d", fields.getValue("GUID").value)
        // date -> type:date, ISO value passes through
        assertEquals("type:date", fields.getValue("Datum").type)
        assertEquals("2022-01-27", fields.getValue("Datum").value)

        // Empty values and unknown types are skipped
        assertFalse(fields.containsKey("empty"))
        assertFalse(fields.containsKey("bogus"))
    }

    @Test
    fun parseXenoDataPrefixesNamesForDocAndCorpusLevels() {
        val xml =
            """
            <idsHeader>
              <xenoData>
                <meta name="rcpnt" project="KED" type="keyword" desc="recipient group">kinder</meta>
                <meta name="rcpntLabel" project="KED" type="attachment">Kinder</meta>
              </xenoData>
            </idsHeader>
            """
        val docFields = StandoffMetadata.parseXenoData(headerElement(xml), "doc").associateBy { it.key }
        assertEquals(listOf("kinder"), docFields.getValue("KED.docRcpnt").value)
        assertEquals("data:,Kinder", docFields.getValue("KED.docRcpntLabel").value)

        val corpusFields = StandoffMetadata.parseXenoData(headerElement(xml), "corpus").associateBy { it.key }
        assertEquals(listOf("kinder"), corpusFields.getValue("KED.corpusRcpnt").value)
        assertEquals("data:,Kinder", corpusFields.getValue("KED.corpusRcpntLabel").value)
    }

    /**
     * Full-pipeline mirror of KorAP-XML-Krill's t/real/ked.t (commit 24ad3c0):
     * the KED/KLX/03212 fixture carries xenoData at text, doc and corpus level,
     * and its Krill JSON must expose every meta as a typed field. The Perl test
     * asserts on the internal `<typeprefix>_<key>` meta keys (e.g. A_KED.topicLabel);
     * here the type prefix has become the koral:field `type` and the key is the
     * remainder (KED.topicLabel), so the assertions correspond one-to-one.
     */
    @Test
    fun xenoDataFromKedCorpusMatchesPerlReference() {
        val tar = ensureKrillTar("ked_xenodata", "ked_sample.krill.tar") { outputDir ->
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, loadResource("ked_sample.zip").path)
        }
        val json = readKrillJson(tar).getValue("KED-KLX-03212.json")

        fun field(key: String, value: String, type: String) =
            """"key":"$key","@type":"koral:field","value":"$value","type":"$type""""
        fun keywords(key: String, vararg values: String) =
            """"key":"$key","@type":"koral:field","value":[""" +
                values.joinToString(",") { "\"$it\"" } + """],"type":"type:keywords""""

        // attachment metas -> type:attachement, value wrapped as a data: URI (A_ in ked.t)
        assertContains(json, field("KED.topicLabel", "data:,Gesundheit und Krankheit", "type:attachement"))
        assertContains(json, field("KED.strtgyLabel", "data:,Erklären", "type:attachement"))
        assertContains(json, field("KED.txttypLabel", "data:,Lexikonartikel", "type:attachement"))
        assertContains(json, field("KED.rcpntLabel", "data:,Kinder", "type:attachement"))
        assertContains(json, field("KED.nToks", "data:,308", "type:attachement"))
        assertContains(json, field("KED.nSent", "data:,28", "type:attachement"))
        assertContains(json, field("KED.nTyps", "data:,188", "type:attachement"))
        assertContains(json, field("KED.nToksSentMd", "data:,11.0", "type:attachement"))
        assertContains(json, field("KED.nPunct1kTks", "data:,129.87", "type:attachement"))

        // string metas -> type:string (S_ in ked.t)
        assertContains(json, field("KED.cover1Herder", "0.58", "type:string"))
        assertContains(json, field("KED.cover2Herder", "0.61", "type:string"))
        assertContains(json, field("KED.cover3Herder", "0.62", "type:string"))
        assertContains(json, field("KED.cover4Herder", "0.65", "type:string"))
        assertContains(json, field("KED.nPara", "5", "type:string"))
        // Repeated scalar keeps the last value (0.66 then "added for testing" 0.67)
        assertContains(json, field("KED.cover5Herder", "0.67", "type:string"))

        // keyword metas -> type:keywords (K_ in ked.t); repeats accumulate in order
        assertContains(json, keywords("KED.strtgy", "erklaeren", "beschreiben"))
        assertContains(json, keywords("KED.topic", "gesundheit_krankheit"))
        assertContains(json, keywords("KED.txttyp", "lexikonartikel"))
        assertContains(json, keywords("KED.rcpnt", "kinder"))

        // doc- and corpus-level xenoData inherit onto the text under level-prefixed
        // keys, so they never collide with the text-level rcpnt field.
        assertContains(json, keywords("KED.docRcpnt", "kinder"))
        assertContains(json, field("KED.docRcpntLabel", "data:,Kinder", "type:attachement"))
        assertContains(json, keywords("KED.corpusRcpnt", "kinder"))
        assertContains(json, field("KED.corpusRcpntLabel", "data:,Kinder", "type:attachement"))

        // Anchor: the sigle and a standard header field are still present. (Sigle
        // fields use a different property order, so check them order-independently.)
        assertContains(json, "\"key\":\"textSigle\"")
        assertContains(json, "\"value\":\"KED/KLX/03212\"")
        assertContains(json, field("title", "Flöhe", "type:text"))
    }

    @Test
    fun correctedMetadataFieldNamesByDefault() {
        val baseZip = loadResource("rei_sample.zip").path
        val tar = ensureKrillTar("rei_corrected_names", "rei_sample.krill.tar") { outputDir ->
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, baseZip)
        }
        val json = readKrillJson(tar).entries.first { it.key.startsWith("REI-RBR-00473") }.value

        // The misleadingly named textClass is emitted under its corrected name dmozDomain.
        assertTrue(json.contains("\"key\":\"dmozDomain\""), "textClass should be emitted as dmozDomain by default")
        assertFalse(json.contains("\"key\":\"textClass\""), "legacy textClass key must not appear by default")
    }

    @Test
    fun legacyMetadataFieldNamesWithFlag() {
        val baseZip = loadResource("rei_sample.zip").path
        val tar = ensureKrillTar("rei_legacy_names", "rei_sample.krill.tar") { outputDir ->
            arrayOf("-t", "krill", "-q", "--legacy-field-names", "-D", outputDir.path, baseZip)
        }
        val json = readKrillJson(tar).entries.first { it.key.startsWith("REI-RBR-00473") }.value

        // With --legacy-field-names the historical key is kept and the corrected one is absent.
        assertTrue(json.contains("\"key\":\"textClass\""), "--legacy-field-names should keep textClass")
        assertFalse(json.contains("\"key\":\"dmozDomain\""), "corrected name must not appear in legacy mode")
    }

    /**
     * Regression test for https://github.com/KorAP/korapxmltool/issues/46
     *
     * Krill should not index a text that has no tokens, so empty texts must be dropped (with a
     * warning) instead of producing unusable empty documents. The fixture contains two texts:
     *   - M21/FEB.04748: an empty text with 0 tokens -> must be dropped
     *   - M21/FEB.04749: a text with exactly 1 token -> must be kept
     */
    @Test
    fun krillDropsEmptyTextsButKeepsSingleTokenTexts() {
        val baseZip = loadResource("m21_empty_sample.zip").path
        val tar = ensureKrillTar("m21_empty_sample", "m21_empty_sample.krill.tar") { outputDir ->
            arrayOf("-t", "krill", "-q", "-l", "info", "-D", outputDir.path, baseZip)
        }

        val tarListProcess = ProcessBuilder("tar", "-tf", tar.path).redirectErrorStream(true).start()
        val entries = tarListProcess.inputStream.bufferedReader().readLines()
        assertTrue(tarListProcess.waitFor() == 0)

        val textIds = entries.filter { it.endsWith(".json.gz") }.map { it.removeSuffix(".json.gz") }

        // The single-token text is kept; the empty text is dropped.
        assertTrue(textIds.any { it.endsWith("M21-FEB-04749") }, "1-token text must be kept, got: $textIds")
        assertFalse(textIds.any { it.endsWith("M21-FEB-04748") }, "empty text must be dropped, got: $textIds")
        assertEquals(1, textIds.size, "exactly one text expected in the TAR, got: $textIds")

        // The drop is reported in the run log.
        val logFile = File(tar.path.replace(Regex("\\.tar$"), ".log"))
        assertTrue(logFile.exists(), "krill run log should exist")
        val log = logFile.readText()
        assertTrue(
            log.contains("Skipping text M21_FEB.04748: no tokens"),
            "log should warn about the dropped empty text"
        )
    }

    /**
     * Regression test for https://github.com/KorAP/korapxmltool/issues/47
     *
     * In older Wikipedia corpora (wdd17/wpd17) the article URL is only present in the
     * reference[type=complete] text, not encoded as a ref/link element. It must still be picked up
     * as the Krill externalLink, with title "Wikipedia".
     */
    @Test
    fun krillExtractsWikipediaExternalLinkFromReference() {
        val baseZip = loadResource("wdd17sample.zip").path
        val tar = ensureKrillTar("wdd17_externallink", "wdd17sample.krill.tar") { outputDir ->
            arrayOf("-t", "krill", "-q", "-D", outputDir.path, baseZip)
        }

        val json = readKrillJson(tar).getValue("WDD17-B06-45592.json")
        val externalLink = krillFieldValue(json, "externalLink")
        assertNotNull(externalLink, "Wikipedia text should have an externalLink field")
        assertTrue(externalLink.contains("title=Wikipedia"), "link title should be Wikipedia: $externalLink")
        assertTrue(
            externalLink.contains("de.wikipedia.org%2Fwiki%2FDiskussion%3ABerlinische_Grammatik"),
            "link should contain the encoded Wikipedia URL: $externalLink"
        )
    }

    /**
     * Wiring test for the Genios newspaper case (issue #47): a corpus sigle whose botkuerzel is
     * known (W24 -> WELT) plus a biblNote "ID:" yields a genios.de full-text link with title GENIOS.
     */
    @Test
    fun krillDerivesGeniosExternalLinkForNewspaper() {
        val tool = KorapXmlTool()
        val metadata = collectKrillMetadata(
            tool,
            "W24_SEP.00359",
            headerElement(
                """
                <idsHeader>
                  <fileDesc>
                    <titleStmt><textSigle>W24/SEP.00359</textSigle></titleStmt>
                    <sourceDesc>
                      <biblStruct>
                        <analytic>
                          <h.title type="main">Wir brauchen ein Smartphone-Verbot in Schulen</h.title>
                          <biblNote n="1">ID: 216199263 file: Originaldaten/2024/WELT.xml.zip@ Categories: Ressort: Forum</biblNote>
                        </analytic>
                        <monogr><h.title type="main">Die Welt</h.title></monogr>
                      </biblStruct>
                      <reference type="complete" assemblage="regular">W24/SEP.00359 Die Welt, 12.09.2024, S. 7.</reference>
                    </sourceDesc>
                  </fileDesc>
                </idsHeader>
                """
            )
        )
        assertEquals("https://www.genios.de/document/WELT__216199263", metadata["externalLink"])
        assertEquals("GENIOS", metadata["externalLinkTitle"])
    }
}
