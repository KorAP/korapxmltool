package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.net.URL
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Tests for Word2Vec format output (-t w2v)
 */
class Word2VecFormatterTest {
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
    fun deprecatedW2vOptionNoLongerWorks() {
        val args = arrayOf("-w", loadResource("wdf19.zip").path)
        val exitCode = debug(args)
        assertTrue(exitCode != 0, "Old -w option should no longer work in v3.0")
    }

    @Test
    fun w2vOptionWorks() {
        val args = arrayOf("-t", "w2v", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(outContent.toString(), "\nje ne suis pas du tout d'accord !\n")
        assertFalse { outContent.toString().contains("WDF19_A0000.13865") }
    }

    @Test
    fun canHandleInvalidXmlComments() {
        val zca20scrambled = loadResource("zca20-scrambled.zip").path
        val args = arrayOf("-t", "w2v", zca20scrambled)
        debug(args)
        assertContains(
            outContent.toString(),
            "\nDys est yuch dyr Grund dyfür , dyss ys schon myl myhryry Wochyn dyuyrn kynn .\n"
        )
    }

    @Test
    fun canWord2VecLemma() {
        val args = arrayOf("--lemma", "-t", "w2v", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        assertContains(out, " mein Ankunft ")
    }

    @Test
    fun lemmaOnlyWord2VecWorks() {
        val args = arrayOf("--lemma-only", "-t", "w2v", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        assertTrue(out.contains(" mein ") || out.contains(" Ankunft "))
    }

    @Test
    fun w2vCanExtractMetadata() {
        val args = arrayOf(
            "-t", "w2v",
            "-m", "<textSigle>([^<]+)",
            "-m", "<creatDate>([^<]+)",
            loadResource("wdf19.zip").path
        )
        debug(args)
        assertContains(
            outContent.toString(),
            "WDF19/A0000.12006\t2011.08.11\tmerci pour l'info je suis curieux !"
        )
    }

    @Test
    fun w2vCanHandleNonBmpText() {
        val wdd17 = loadResource("wdd17sample.zip").path
        val args = arrayOf("-t", "w2v", wdd17)
        debug(args)
        assertContains(outContent.toString(), "\n-- mach \uD83D\uDE48 \uD83D\uDE49 \uD83D\uDE4A 20 : 45 , 1. Feb .\n")
        assertContains(outContent.toString(), "\nBereinige wenigstens die allergröbsten Sachen .\n")
    }

    @Test
    fun w2vExcludeZipGlobSkipsFiles() {
        val args = arrayOf(
            "--exclude-zip-glob", "goe.zip",
            "-t", "w2v",
            loadResource("wdf19.zip").path,
            loadResource("goe.zip").path
        )
        debug(args)
        val out = outContent.toString()
        assertContains(out, "automatique")
        assertFalse(out.contains("Gedanken"))
    }
}
