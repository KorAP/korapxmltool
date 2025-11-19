package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.net.URL
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertTrue

/**
 * Tests for NOW (News on Web) format output (-t now)
 */
class NowFormatterTest {
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
    fun nowOptionWorks() {
        val args = arrayOf("-t", "now", loadResource("wdf19.zip").path)
        debug(args)
        val output = outContent.toString()
        assertContains(output, "@@WDF19_A0000.")
        assertContains(output, " <p> ")
        assertContains(output, "Arts visuels Pourquoi toujours vouloir")
        assertTrue(!output.contains("# foundry"))
        val lines = output.trim().split('\n')
        assertTrue(lines.all { it.startsWith("@@") })
    }

    @Test
    fun canNowLemma() {
        val args = arrayOf("--lemma", "-t", "now", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        assertContains(out, "@@")
        assertContains(out, " <p> ")
        assertContains(out, " mein Ankunft ")
    }

    @Test
    fun lemmaOnlyNowWorks() {
        val args = arrayOf("--lemma-only", "-t", "now", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        assertContains(out, "@@")
        assertContains(out, " <p> ")
    }

    @Test
    fun sequentialOnlyForNowAndW2V() {
        val args = arrayOf("--sequential", loadResource("wdf19.zip").path)
        val rc = debug(args)
        assertTrue(rc != 0)
        assertContains(errContent.toString(), "--sequential is supported only with -t word2vec or -t now")
    }
}
