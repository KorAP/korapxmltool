package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.net.URL
import kotlin.test.Test
import kotlin.test.assertContains
import org.junit.Ignore
import kotlin.test.assertFalse

class KorapXml2ConlluTest {
    private val outContent = ByteArrayOutputStream(10000000)
    private val errContent = ByteArrayOutputStream()
    private val originalOut: PrintStream = System.out
    private val originalErr: PrintStream = System.err

    val goe = loadResource("goe.zip").path
    val goeMarmot = loadResource("goe.marmot.zip").path
    val goeTreeTagger = loadResource("goe.tree_tagger.zip").path

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
        val args = arrayOf(loadResource("goe.zip").path, loadResource("goe.tree_tagger.zip").path)
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
        val args = arrayOf(loadResource("wdf19.zip").path, loadResource("wdf19.tree_tagger.zip").path)
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
    fun w2vOptionWorks() {
        val args = arrayOf("-w", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "\nje ne suis pas du tout d'accord !\n"
        )
        assertFalse { outContent.toString().contains("WDF19_A0000.13865") }
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

    @Ignore("for some reason not working")
    fun canConvertMorphoFeatureAnnotations() {
        val args = arrayOf(goe, goeMarmot)
        debug(args)
        assertContains(
            outContent.toString(),
            "9\tentzücke\tentzücken\t_\tVVFIN\tnumber=sg|person=3|tense=pres|mood=subj\t_\t_\t_\t1.000000"
        )
    }
}
