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

class KorapXml2ConlluTest {
    private val outContent = ByteArrayOutputStream(10000000)
    private val errContent = ByteArrayOutputStream()
    private val originalOut: PrintStream = System.out
    private val originalErr: PrintStream = System.err

    val goe = loadResource("goe.zip").path
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
            "31\tvraie\tvrai\t_\tADJ\t_\t_\t_\t_\t1.000000"
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
        val tmpSourceFileName = createTempFile("tmp", ".zip", null).absolutePath
        File(sourceFile).copyTo(File(tmpSourceFileName), true)

        val args = arrayOf("-o", "-f", "zip", tmpSourceFileName)
        debug(args)

        val resultFile = tmpSourceFileName.toString().replace(".zip", ".base.zip")
        assert(File(resultFile).exists())
    }

    @Test
    fun overwriteWorks() {
        val sourceFile = loadResource("wdf19.zip").path
        val tmpSourceFileName = createTempFile("tmp", ".zip", null).absolutePath
        File(sourceFile).copyTo(File(tmpSourceFileName), true)
        val resultFile = tmpSourceFileName.toString().replace(".zip", ".base.zip")
        File(resultFile).createNewFile()
        val args = arrayOf("-o", "-f", "zip", tmpSourceFileName)
        debug(args)
        assert(File(resultFile).exists())
        assert(File(resultFile).length() > 0)
     }
}
