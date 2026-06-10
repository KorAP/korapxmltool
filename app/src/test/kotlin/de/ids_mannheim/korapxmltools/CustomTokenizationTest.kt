package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import java.util.zip.GZIPInputStream
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Tests for corpora with custom tokenization and annotations shipped inside
 * the base ZIP (e.g. TEI conversions with their own <w>-level POS/lemma
 * annotations, stored in a custom foundry folder like cmc/morpho.xml instead
 * of base/tokens.xml).
 *
 * The dck_sample.zip resource is a two-text excerpt from the CC BY licensed
 * Dortmunder Chat-Korpus (DCK); in text DCK/CPR/00004 the sentence (s) spans
 * were removed from structure.xml to exercise the sentence-segmentation
 * fallback to <posting> elements.
 */
class CustomTokenizationTest {
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
    fun conlluUsesFoundryFromAnnotationFolderName() {
        val args = arrayOf(loadResource("dck_sample.zip").path)
        debug(args)
        val output = outContent.toString()
        assertContains(output, "# foundry = cmc")
        assertFalse(output.contains("# foundry = base"), "Custom annotations must not be reported as base foundry")
        assertContains(output, "cmc/morpho.xml")
        // POS and lemma from the custom morpho.xml must survive
        assertContains(output, "begrüssen\tbegrüßen\t_\tVVFIN")
    }

    @Test
    fun conlluFallsBackToPostingsWithoutSentenceSpans() {
        val args = arrayOf("-l", "info", loadResource("dck_sample.zip").path)
        debug(args)
        assertContains(errContent.toString(), "falling back to 145 <posting> elements")

        // Text 00004 has no s spans: its tokens must still be split into
        // sentences (one per posting) instead of one giant sentence
        val text4 = outContent.toString().substringAfter("# text_id = DCK_CPR.00004")
        val sentenceBreaks = text4.split("\n\n").size - 1
        assertTrue(sentenceBreaks > 100, "Expected ~145 posting-based sentences, got $sentenceBreaks breaks")
    }

    @Test
    fun krillCollectsCustomFoundryAnnotationsAndTokens() {
        val outputDir = File.createTempFile("dck_krill", "").apply {
            delete()
            mkdirs()
        }
        try {
            val args = arrayOf("-t", "krill", "-q", "-D", outputDir.path, loadResource("dck_sample.zip").path)
            assertEquals(0, debug(args))

            val tar = File(outputDir, "dck_sample.krill.tar")
            assertTrue(tar.exists(), "Expected dck_sample.krill.tar")

            val jsons = readKrillJsons(tar)
            val json1 = jsons.getValue("DCK-CPR-00001.json")
            val json4 = jsons.getValue("DCK-CPR-00004.json")

            jsons.values.forEach { json ->
                // Annotations must be filed under the folder-derived cmc foundry
                assertContains(json, "cmc cmc/morpho")
                assertContains(json, "cmc/p=tokens")
                assertContains(json, "cmc/l=tokens")
                assertContains(json, "\"cmc/p:")
                assertContains(json, "\"cmc/l:")
                // Tokens come from cmc/morpho.xml (no base/tokens.xml in the corpus)
                assertContains(json, "\"value\":\"cmc#morpho\"")
            }

            // Surface forms must be filled from data.xml
            assertContains(json1, "\"s:begrüssen\"")
            assertContains(json4, "\"s:ich\"")

            assertContains(json1, "-:base/sentences\$<i>184")
            // Text 00004 has no s spans: sentence count falls back to postings
            assertContains(json4, "-:base/sentences\$<i>145")
            assertContains(json4, "-:base/paragraphs\$<i>145")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    private fun readKrillJsons(tarFile: File): Map<String, String> {
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
