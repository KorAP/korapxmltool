package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.net.URL
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Tests for CoNLL-U format output (default format)
 */
class ConlluFormatterTest {
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
    fun canConvertBasicZipToConllu() {
        val args = arrayOf(loadResource("goe.zip").path)
        debug(args)
        assertContains(outContent.toString(), "# foundry = base")
        assertContains(
            outContent.toString(),
            "# start_offsets = 55 55 59 63 70 75 82 87 94 102 105 111 120 124 130 134 140 144 151 153 163 175 187 191 207 209 213 218 222 239 248 255 259 264 267 271 277 283 297 307"
        )
    }

    @Test
    fun canConvertWithMorphoAnnotations() {
        val args = arrayOf(loadResource("goe.tree_tagger.zip").path)
        debug(args)
        assertContains(outContent.toString(), "# foundry = tree_tagger")
        assertContains(outContent.toString(), "9\tentzücke\tentzücken\t_\tVVFIN\t_\t_\t_\t_\t_")
    }

    @Test
    fun canInferFoundryFromFilename() {
        val goeTreeTagger = loadResource("goe.tree_tagger.zip").path
        val args = arrayOf(goeTreeTagger)
        debug(args)
        assertContains(outContent.toString(), "# foundry = tree_tagger")
        assertContains(outContent.toString(), "9\tentzücke\tentzücken\t_\tVVFIN\t_\t_\t_\t_\t_")
    }

    @Test
    fun canConvertWithFrenchAnnotations() {
        val args = arrayOf(loadResource("wdf19.tree_tagger.zip").path)
        debug(args)
        assertContains(outContent.toString(), "# foundry = tree_tagger")
        assertContains(outContent.toString(), "\tvraie\tvrai\t_\tADJ\t_\t_\t_\t_\t")
    }

    @Test
    fun respectsSiglePattern() {
        val args = arrayOf("-p", ".*7", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(outContent.toString(), "# text_id = WDF19_A0000.14247")
        assertFalse { outContent.toString().contains("WDF19_A0000.13865") }
    }

    @Test
    fun respectsColumnsParam() {
        val args = arrayOf("-c", "5", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(outContent.toString(), "42\tparfaitement\t_\t_\t_\n")
    }

    @Test
    fun respectsSpecial1ColumnsParam() {
        val args = arrayOf("-c", "1", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(outContent.toString(), "\nparfaitement\n")
    }

    @Test
    fun canConvertMultipleZips() {
        val wdf19 = loadResource("wdf19.zip").path
        val goe = loadResource("goe.zip").path
        val args = arrayOf(wdf19, goe)
        debug(args)
        assertContains(outContent.toString(), "6\tautomatique\t_\t_\t_\t_\t_\t_\t_\t_\n")
        assertContains(outContent.toString(), "36\tGedanken\t_\t_\t_\t_\t_\t_\t_\t_\n")
    }

    @Test
    fun canConvertMorphoFeatureAnnotations() {
        val goeMarmot = loadResource("goe.marmot.zip").path
        val args = arrayOf(goeMarmot)
        debug(args)
        assertContains(
            outContent.toString(),
            "9\tentzücke\t_\t_\tVVFIN\tnumber=sg|person=3|tense=pres|mood=subj\t_\t_\t_\t_\n"
        )
    }

    @Test
    fun dependencyColumnsArePopulatedFromSpacyZip() {
        val goeSpacy = loadResource("goe.spacy.zip").path
        val args = arrayOf(goeSpacy)
        debug(args)
        val out = outContent.toString()

        assertContains(out, "# foundry = spacy")
        assertContains(out, "# text_id = GOE_AGA.00000")

        val dataLines = out.lines().filter { !it.startsWith("#") && it.isNotBlank() }
        assertTrue(dataLines.isNotEmpty(), "Should have data lines in output")

        var tokensWithHead = 0
        var tokensWithDeprel = 0
        var totalTokens = 0

        for (line in dataLines) {
            val columns = line.split(Regex("\\s+"))
            if (columns.size >= 8) {
                totalTokens++
                val head = columns[6]
                val deprel = columns[7]
                if (head != "_") tokensWithHead++
                if (deprel != "_") tokensWithDeprel++
            }
        }

        assertTrue(totalTokens > 0, "Should have parsed at least some tokens")

        val headCoverage = (tokensWithHead.toDouble() / totalTokens) * 100
        assertTrue(
            headCoverage > 40.0,
            "HEAD column should be populated for significant portion of tokens. Found: $tokensWithHead/$totalTokens (${headCoverage}%)"
        )

        val deprelCoverage = (tokensWithDeprel.toDouble() / totalTokens) * 100
        assertTrue(
            deprelCoverage > 40.0,
            "DEPREL column should be populated for significant portion of tokens. Found: $tokensWithDeprel/$totalTokens (${deprelCoverage}%)"
        )

        assertTrue(
            out.contains(Regex("\\n\\d+\\t\\S+\\t\\S+\\t\\S+\\t\\S+\\t\\S+\\t\\d+\\t\\S+\\t")),
            "Should find tokens with numeric HEAD values in column 7"
        )
    }

    @Test
    fun conlluIncludesConstituencyCommentsWhenAvailable() {
        outContent.reset()
        errContent.reset()

        val wud24Corenlp = loadResource("wud24_sample.corenlp.zip").path
        val args = arrayOf(wud24Corenlp)
        val exitCode = debug(args)
        assertEquals(0, exitCode, "CoNLL-U conversion should succeed when constituency annotations are present")

        val output = outContent.toString("UTF-8")
        val constituencyLines = output.lineSequence().filter { it.startsWith("# constituency =") }.toList()

        assertTrue(constituencyLines.isNotEmpty(), "CoNLL-U output should include constituency comment lines")
        assertTrue(
            constituencyLines.first().contains("("),
            "Constituency comment should contain bracketed structure"
        )
    }

    @Test
    fun canExtractExtraFeaturesByRegex() {
        val args = arrayOf("-e", "(posting/id|div/id)", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "12\t)\t_\t_\t_\t_\t_\t_\t_\t_\n" +
                    "# div/id = i.14293_8\n" +
                    "13\tDifférentiation\t_\t_\t_\t_\t_\t_\t_\t_\n" +
                    "# posting/id = i.14293_8_1\n" +
                    "14\tAinsi\t_\t_\t_\t_\t_\t_\t_\t_\n"
        )
    }
}
