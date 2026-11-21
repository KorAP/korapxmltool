package de.ids_mannheim.korapxmltools

import org.apache.commons.compress.archivers.zip.ZipFile
import org.junit.After
import org.junit.Assume
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.util.logging.Logger
import java.util.zip.GZIPInputStream
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CoreNLPNerBridgeTest {
    private val outContent = ByteArrayOutputStream()
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

    private val modelName = "german.conll.germeval2014.hgc_175m_600.crf.ser.gz"

    // The German NER model is not in the repository; see the Readme for where to get it
    private fun modelFile(): File {
        val model = listOf(File("libs/$modelName"), File("../libs/$modelName")).firstOrNull { it.exists() }
        Assume.assumeTrue("CoreNLP NER model $modelName not found", model != null)
        return model!!
    }

    private fun createTempDir(prefix: String): File =
        File.createTempFile(prefix, "").apply { delete(); mkdirs() }

    private fun sentence(): Triple<NonBmpString, Array<KorapXmlTool.Span>, Array<KorapXmlTool.Span>> {
        val tokens = listOf("Konrad", "Alberti", "lebte", "in", "Berlin", ".")
        val text = tokens.joinToString(" ")
        var from = 0
        val spans = tokens.map { token -> KorapXmlTool.Span(from, from + token.length).also { from += token.length + 1 } }
        return Triple(NonBmpString(text), spans.toTypedArray(), arrayOf(KorapXmlTool.Span(0, text.length)))
    }

    @Test
    fun normalizesVerboseCoreNlpClassNamesToShortStandardTags() {
        assertEquals("PER", CoreNLPNerBridge.normalizeTag("PERSON"))
        assertEquals("LOC", CoreNLPNerBridge.normalizeTag("LOCATION"))
        assertEquals("ORG", CoreNLPNerBridge.normalizeTag("ORGANIZATION"))
        assertEquals("MISC", CoreNLPNerBridge.normalizeTag("MISC"))
        assertEquals("I-PER", CoreNLPNerBridge.normalizeTag("I-PER"))
        assertEquals("B-PER", CoreNLPNerBridge.normalizeTag("B-PERSON"))
        assertNull(CoreNLPNerBridge.normalizeTag("O"))
    }

    @Test
    fun tagsBaseTokensWithShortStandardTags() {
        val ner = CoreNLPNerBridge(modelFile().path, Logger.getAnonymousLogger())
        val (text, spans, sentences) = sentence()

        val entities = ner.tagText(spans, sentences, text)

        fun tagOf(i: Int) = entities["${spans[i].from}-${spans[i].to}"]?.ne
        assertEquals("PER", tagOf(0), "Konrad")
        assertEquals("PER", tagOf(1), "Alberti")
        assertNull(tagOf(2), "Tokens outside entities get no span")
        assertEquals("LOC", tagOf(4), "Berlin")
    }

    @Test
    fun classifierGetsTheFeaturesTheModelWasTrainedWith() {
        // With CRFClassifier.classify on bare CoreLabels (no word shapes etc.), the model
        // missed all three entities here; recall on GermEval 2014 dropped from 72% to 25%
        val ner = CoreNLPNerBridge(modelFile().path, Logger.getAnonymousLogger())
        val tokens = listOf("Marion", "Bornscheuer", "arbeitet", "für", "die", "Wikimedia", "Foundation", "und", "EADS", ".")
        val text = tokens.joinToString(" ")
        var from = 0
        val spans = tokens.map { token -> KorapXmlTool.Span(from, from + token.length).also { from += token.length + 1 } }

        val entities = ner.tagText(spans.toTypedArray(), arrayOf(KorapXmlTool.Span(0, text.length)), NonBmpString(text))

        assertEquals(listOf("PER", "PER", null, null, null, "ORG", "ORG", null, "ORG", null),
            spans.map { entities["${it.from}-${it.to}"]?.ne })
    }

    @Test
    fun nerWritesMultiTokenSpansThatKrillIndexesAsSpansAndTokens() {
        val model = modelFile()
        val dir = createTempDir("corenlp_ner")
        try {
            val baseZip = Thread.currentThread().contextClassLoader.getResource("wud24_sample.zip")!!.path
            assertEquals(0, debug(arrayOf("-q", "-N", "corenlp:${model.path}", "-t", "zip", "-D", dir.path, baseZip)))

            val neZip = File(dir, "wud24_sample.corenlp.zip")
            val entries = ZipFile.builder().setFile(neZip).get().use { zip -> zip.entries.asSequence().map { it.name }.toList() }
            assertEquals(3, entries.count { it.endsWith("/corenlp/ne.xml") }, "Every text gets an ne layer: $entries")
            assertFalse(entries.any { it.endsWith("morpho.xml") }, "NER alone writes no morpho layer: $entries")

            val neXml = ZipFile.builder().setFile(neZip).get().use { zip ->
                zip.getInputStream(zip.getEntry("WUD24/I0083/95367/corenlp/ne.xml")).bufferedReader().readText()
            }
            assertContains(neXml, ">ORG<")
            assertFalse(neXml.contains(">ORGANIZATION<"), "CoreNLP tags should be normalized to PER/LOC/ORG/MISC")

            assertEquals(0, debug(arrayOf("-q", "-t", "krill", "-D", dir.path, baseZip, neZip.path)))
            val extractDir = File(dir, "krill").apply { mkdirs() }
            assertEquals(0, ProcessBuilder("tar", "-xf", File(dir, "wud24_sample.krill.tar").path, "-C", extractDir.path).start().waitFor())
            val json = GZIPInputStream(File(extractDir, "WUD24-I0083-95367.json.gz").inputStream()).bufferedReader().readText()
            assertContains(json, "\"corenlp/ne:ORG\"")
            assertContains(json, "\"<>:corenlp/ne:ORG\$<b>64<i>")
            assertContains(json.substringAfter("\"layerInfos\":").substringBefore("\","), "corenlp/ne=tokens")
            assertFalse(json.substringAfter("\"layerInfos\":").substringBefore("\",").contains("corenlp/ne=spans"),
                "layerInfos should keep ne=tokens only so Kalamar does not duplicate span+token terms in tree view")
        } finally {
            dir.deleteRecursively()
        }
    }

    @Test
    fun nerRequiresKorapXmlZipOutput() {
        val dir = createTempDir("corenlp_ner_conllu")
        try {
            // Only the model file's existence is checked before the output format is validated
            val dummyModel = File(dir, "dummy.crf.ser.gz").apply { writeText("") }
            val baseZip = Thread.currentThread().contextClassLoader.getResource("wud24_sample.zip")!!.path
            assertTrue(debug(arrayOf("-N", "corenlp:${dummyModel.path}", "-t", "conllu", baseZip)) != 0,
                "NER results would be silently lost in CoNLL-U output")
            assertTrue(debug(arrayOf("-N", "corenlp:${dummyModel.path}", "-A", "cat", "-t", "zip", "-D", dir.path, baseZip)) != 0,
                "NER results would be silently lost with an external annotator")
        } finally {
            dir.deleteRecursively()
        }
    }
}
