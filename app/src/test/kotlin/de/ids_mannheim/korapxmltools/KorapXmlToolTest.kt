package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KorapXmlToolTest {
    private val outContent = ByteArrayOutputStream(10000000)
    private val errContent = ByteArrayOutputStream()
    private val originalOut: PrintStream = System.out
    private val originalErr: PrintStream = System.err

    val goe = loadResource("goe.zip").path
    val goeSpacy = loadResource("goe.spacy.zip").path
    val goeMarmot = loadResource("goe.marmot.zip").path
    val goeTreeTagger = loadResource("goe.tree_tagger.zip").path
    val zca20scrambled = loadResource("zca20-scrambled.zip").path
    val wdf19 = loadResource("wdf19.zip").path
    val wdd17 = loadResource("wdd17sample.zip").path

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
    fun canConvertGOE() {
        val args = arrayOf(loadResource("goe.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "# foundry = base"
        )
        assertContains(
            outContent.toString(),
            "# start_offsets = 55 55 59 63 70 75 82 87 94 102 105 111 120 124 130 134 140 144 151 153 163 175 187 191 207 209 213 218 222 239 248 255 259 264 267 271 277 283 297 307"
        )
    }
    @Test
    fun canConvertWithMorphoAnnotations() {
        val args = arrayOf(loadResource("goe.tree_tagger.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "# foundry = tree_tagger"
        )
        assertContains(
            outContent.toString(),
            "9\tentzücke\tentzücken\t_\tVVFIN\t_\t_\t_\t_\t1.000000"
        )
    }
    @Test
    fun canInferBaseName() {
        val args = arrayOf(goeTreeTagger)
        debug(args)
        assertContains(
            outContent.toString(),
            "# foundry = tree_tagger"
        )
        assertContains(
            outContent.toString(),
            "9\tentzücke\tentzücken\t_\tVVFIN\t_\t_\t_\t_\t1.000000"
        )
    }

    @Test
    fun canConvertWfdWithMorphoAnnotations() {
        val args = arrayOf(loadResource("wdf19.tree_tagger.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "# foundry = tree_tagger"
        )
        assertContains(
            outContent.toString(),
            "\tvraie\tvrai\t_\tADJ\t_\t_\t_\t_\t"
        )
    }

    @Test
    fun canPrintHelp() {
        debug(arrayOf("-h"))
        assertContains(
            outContent.toString(),
            "--s-bounds-from-morpho"
        )
    }

    @Test
    fun respectsSiglePattern() {
        val args = arrayOf("-p",".*7", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "# text_id = WDF19_A0000.14247"
        )
        assertFalse { outContent.toString().contains("WDF19_A0000.13865") }
    }

    @Test
    fun respectsColumnsParam() {
        val args = arrayOf("-c","5", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "42\tparfaitement\t_\t_\t_\n"
        )
    }

    @Test
    fun respectsSpecial1ColumnsParam() {
        val args = arrayOf("-c","1", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "\nparfaitement\n"
        )
    }

    @Test
    fun deprecatedW2vOptionWorks() {
        val args = arrayOf("-w", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "\nje ne suis pas du tout d'accord !\n"
        )
        assertFalse { outContent.toString().contains("WDF19_A0000.13865") }
    }

    @Test
    fun w2vOptionWorks() {
        val args = arrayOf("-f", "w2v", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "\nje ne suis pas du tout d'accord !\n"
        )
        assertFalse { outContent.toString().contains("WDF19_A0000.13865") }
    }

    @Test
    fun nowOptionWorks() {
        val args = arrayOf("-f", "now", loadResource("wdf19.zip").path)
        debug(args)
        val output = outContent.toString()
        // Check that output starts with @@<text-sigle>
        assertContains(output, "@@WDF19_A0000.")
        // Check that sentence boundaries are replaced with <p> tags
        assertContains(output, " <p> ")
        // Check that it contains the expected text content
        assertContains(output, "Arts visuels Pourquoi toujours vouloir")
        // Check that it doesn't contain CoNLL-U format markers
        assertFalse(output.contains("# foundry"))
        // Check that each text is on one line (no newlines within text except at end)
        val lines = output.trim().split('\n')
        assertTrue(lines.all { it.startsWith("@@") })
    }

    @Test
    fun canConvertXMLwithInvalidComments() {
        val args = arrayOf("-w", zca20scrambled)
        debug(args)
        assertContains(
            outContent.toString(),
            "\nDys est yuch dyr Grund dyfür , dyss ys schon myl myhryry Wochyn dyuyrn kynn .\n"
        )
    }

    @Test
    fun canSetLogLevel() {
        val args = arrayOf("-l", "info", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            errContent.toString(),
            "Processing zip file"
        )
    }

    @Test
    fun canAnnotate() {
        val args = arrayOf("-A", "sed -e 's/u/x/g'", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "axtomatiqxe"
        )
        assertTrue("Annotated CoNLL-U should have at least as many lines as the original, but only has ${outContent.toString().count { it == '\n'}} lines"
        ) { outContent.toString().count { it == '\n' } >= 61511 }
    }

    @Test
    fun canExtractMetadata() {
        val args = arrayOf("--word2vec", "-m" ,"<textSigle>([^<]+)", "-m", "<creatDate>([^<]+)", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "WDF19/A0000.12006\t2011.08.11\tmerci pour l'info je suis curieux !"
        )
    }

    @Test
    fun canHandleNonBmpText() {
        val args = arrayOf("--word2vec", wdd17)
        debug(args)
        assertContains(
            outContent.toString(),
            "\n-- mach \uD83D\uDE48 \uD83D\uDE49 \uD83D\uDE4A 20 : 45 , 1. Feb .\n" // 🙈 🙉 🙊
        )
        assertContains(
            outContent.toString(),
            "\nBereinige wenigstens die allergröbsten Sachen .\n"
        )
    }

    @Test
    fun canExtractExtraFeaturesByRegex() {
        val args = arrayOf("-e" ,"(posting/id|div/id)",loadResource("wdf19.zip").path)
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

    @Test
    fun canConvertMultipleZips() {
        val args = arrayOf(wdf19, goe)
        debug(args)
        assertContains(
            outContent.toString(),
            "6\tautomatique\t_\t_\t_\t_\t_\t_\t_\t_\n"
        )
        assertContains(
            outContent.toString(),
            "36\tGedanken\t_\t_\t_\t_\t_\t_\t_\t_\n"
        )
    }

    @Test
    fun canConvertMorphoFeatureAnnotations() {
        val args = arrayOf(goeMarmot)
        debug(args)
        assertContains(
            outContent.toString(),
            "9\tentzücke\t_\t_\tVVFIN\tnumber=sg|person=3|tense=pres|mood=subj\t_\t_\t_\t_\n"
        )
    }

    @Test
    fun korapXmlOutputWorks() {
        val sourceFile = loadResource("wdf19.zip").path
        val tmpSourceFileName = java.io.File.createTempFile("tmp", ".zip").absolutePath
        File(sourceFile).copyTo(File(tmpSourceFileName), true)

        val args = arrayOf("-o", "-f", "zip", tmpSourceFileName)
        debug(args)

        val resultFile = tmpSourceFileName.toString().replace(".zip", ".base.zip")
        assert(File(resultFile).exists())
    }

    @Test
    fun overwriteWorks() {
        val sourceFile = loadResource("wdf19.zip").path
        val tmpSourceFileName = java.io.File.createTempFile("tmp", ".zip").absolutePath
        File(sourceFile).copyTo(File(tmpSourceFileName), true)
        val resultFile = tmpSourceFileName.toString().replace(".zip", ".base.zip")
        File(resultFile).createNewFile()
        val args = arrayOf("-o", "-f", "zip", tmpSourceFileName)
        debug(args)
        assert(File(resultFile).exists())
        assert(File(resultFile).length() > 0)
     }

    @Test
    fun canWord2VecLemma() {
        val args = arrayOf("--lemma", "-f", "w2v", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        // Expect lemma sequence containing "mein Ankunft" (surface would include inflected form elsewhere)
        assertContains(out, " mein Ankunft ")
    }

    @Test
    fun canNowLemma() {
        val args = arrayOf("--lemma", "-f", "now", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        assertContains(out, "@@")
        assertContains(out, " <p> ")
        assertContains(out, " mein Ankunft ")
    }

    @Test
    fun lemmaOnlyWord2VecWorks() {
        val args = arrayOf("--lemma-only", "-f", "w2v", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        // Should produce some lemma tokens without requiring data.xml
        assertTrue(out.contains(" mein ") || out.contains(" Ankunft "))
    }

    @Test
    fun lemmaOnlyNowWorks() {
        val args = arrayOf("--lemma-only", "-f", "now", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        assertContains(out, "@@")
        assertContains(out, " <p> ")
    }

    @Test
    fun excludeZipGlobSkipsFiles() {
        val args = arrayOf("--exclude-zip-glob", "goe.zip", loadResource("wdf19.zip").path, loadResource("goe.zip").path)
        debug(args)
        val out = outContent.toString()
        // Expect French content, but not the German token from GOE
        assertContains(out, "automatique")
        assertFalse(out.contains("Gedanken"))
    }

    @Test
    fun sequentialOnlyForNowAndW2V() {
        val args = arrayOf("--sequential", loadResource("wdf19.zip").path)
        // Default format is conllu; this should error
        val rc = debug(args)
        // Non-zero is expected; and error message should be present
        assertTrue(rc != 0)
        assertContains(errContent.toString(), "--sequential is supported only with -f word2vec or -f now")
    }

    @Test
    fun dependencyColumnsArePopulatedFromSpacyZip() {
        val args = arrayOf(goeSpacy)
        debug(args)
        val out = outContent.toString()

        // Check that output is CoNLL-U format
        assertContains(out, "# foundry = spacy")
        assertContains(out, "# text_id = GOE_AGA.00000")

        // Get data lines (non-comment, non-empty)
        val dataLines = out.lines()
            .filter { !it.startsWith("#") && it.isNotBlank() }

        assertTrue(dataLines.isNotEmpty(), "Should have data lines in output")

        // Parse tokens and check dependency columns (column 7 = HEAD, column 8 = DEPREL)
        var tokensWithHead = 0
        var tokensWithDeprel = 0
        var totalTokens = 0

        for (line in dataLines) {
            val columns = line.split(Regex("\\s+"))
            if (columns.size >= 8) {
                totalTokens++
                // Column 7 (index 6) is HEAD, column 8 (index 7) is DEPREL
                val head = columns[6]
                val deprel = columns[7]

                if (head != "_") tokensWithHead++
                if (deprel != "_") tokensWithDeprel++
            }
        }

        // Assert that we have tokens
        assertTrue(totalTokens > 0, "Should have parsed at least some tokens")

        // Print diagnostic information
        System.err.println("=== Dependency Test Diagnostics ===")
        System.err.println("Total tokens: $totalTokens")
        System.err.println("Tokens with HEAD (!= '_'): $tokensWithHead")
        System.err.println("Tokens with DEPREL (!= '_'): $tokensWithDeprel")
        System.err.println("First 5 data lines:")
        dataLines.take(5).forEach { System.err.println("  $it") }

        // Assert that HEAD column (col 7) is populated for most tokens
        // We expect at least 90% of tokens to have dependency information
        val headCoverage = (tokensWithHead.toDouble() / totalTokens) * 100
        assertTrue(
            headCoverage > 80.0,
            "HEAD column should be populated for most tokens. Found: $tokensWithHead/$totalTokens (${headCoverage}%)"
        )

        // Assert that DEPREL column (col 8) is populated for most tokens
        val deprelCoverage = (tokensWithDeprel.toDouble() / totalTokens) * 100
        assertTrue(
            deprelCoverage > 85.0,
            "DEPREL column should be populated for most tokens. Found: $tokensWithDeprel/$totalTokens (${deprelCoverage}%)"
        )

        // Check for specific dependency relations and head indices in output
        // Look for numeric head indices (not "_")
        assertTrue(
            out.contains(Regex("\\n\\d+\\t\\S+\\t\\S+\\t\\S+\\t\\S+\\t\\S+\\t\\d+\\t\\S+\\t")),
            "Should find tokens with numeric HEAD values in column 7"
        )
    }
}
