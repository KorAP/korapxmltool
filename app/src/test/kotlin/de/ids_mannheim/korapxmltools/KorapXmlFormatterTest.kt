package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests for KorAP XML format output (-f zip or -t zip)
 */
class KorapXmlFormatterTest {
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
    fun korapXmlOutputWorks() {
        val sourceFile = loadResource("wdf19.zip").path
        val tmpSourceFile = File.createTempFile("tmp", ".zip")
        val tmpSourceFileName = tmpSourceFile.absolutePath
        File(sourceFile).copyTo(File(tmpSourceFileName), true)
        val outputDir = File(tmpSourceFileName).parentFile.absolutePath
        val args = arrayOf("-D", outputDir, "-f", "-t", "zip", tmpSourceFileName)
        debug(args)

        val resultFile = tmpSourceFileName.toString().replace(".zip", ".base.zip")
        assertTrue(File(resultFile).exists())
    }

    @Test
    fun overwriteWorks() {
        val sourceFile = loadResource("wdf19.zip").path
        val tmpSourceFile = File.createTempFile("tmp", ".zip")
        val tmpSourceFileName = tmpSourceFile.absolutePath
        File(sourceFile).copyTo(File(tmpSourceFileName), true)
        val resultFile = tmpSourceFileName.toString().replace(".zip", ".base.zip")
        File(resultFile).createNewFile()
        val outputDir = File(tmpSourceFileName).parentFile.absolutePath
        val args = arrayOf("-D", outputDir, "-f", "-t", "zip", tmpSourceFileName)
        debug(args)
        assertTrue(File(resultFile).exists())
        assertTrue(File(resultFile).length() > 0)
    }

    @Test
    fun corenlpConstituencyParsing() {
        val taggerModel = File("libs/german-fast.tagger")
        val parserModel = File("libs/germanSR.ser.gz")

        if (!taggerModel.exists() || !parserModel.exists()) {
            System.err.println("Skipping CoreNLP test: model files not found")
            return
        }

        val baseZip = loadResource("wud24_sample.zip").path
        val outputDir = File.createTempFile("corenlp_test", "").apply {
            delete()
            mkdirs()
        }

        try {
            val args = arrayOf(
                "-t", "zip",
                "-f",
                "-q",
                "-D", outputDir.path,
                "-T", "corenlp:${taggerModel.path}",
                "-P", "corenlp:${parserModel.path}",
                baseZip
            )

            val exitCode = debug(args)
            assertEquals(0, exitCode, "CoreNLP processing should succeed")

            val outputZip = File(outputDir, "wud24_sample.corenlp.zip")
            assertTrue(outputZip.exists(), "Output ZIP should exist at ${outputZip.path}")

            // Get all ZIP entries
            val zipEntries = org.apache.commons.compress.archivers.zip.ZipFile.builder()
                .setFile(outputZip)
                .get()
                .use { zip ->
                    zip.entries.asSequence().map { it.name }.toList()
                }

            val constituencyFiles = zipEntries.filter { it.contains("constituency.xml") }
            assertTrue(constituencyFiles.isNotEmpty(), "Should have constituency.xml files in output")

            val expectedDocs = listOf(
                "WUD24/I0083/95367/corenlp/constituency.xml",
                "WUD24/Z0087/65594/corenlp/constituency.xml",
                "WUD24/K0086/98010/corenlp/constituency.xml"
            )

            expectedDocs.forEach { docPath ->
                val found = constituencyFiles.any { it.contains(docPath) }
                assertTrue(found, "Should have constituency.xml for $docPath")
            }

            val morphoFiles = zipEntries.filter { it.contains("/corenlp/morpho.xml") }
            assertTrue(morphoFiles.size >= 3, "Should have morpho.xml files for at least 3 documents")

        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun externalFoundryDetection() {
        val baseZip = loadResource("wdd17sample.zip").path
        val cmcConlluAnnotation = loadResource("wdd17sample.cmc.conllu").path
        val outputDir = File.createTempFile("external_foundry_test", "").apply {
            delete()
            mkdirs()
        }

        try {
            val args = arrayOf(
                "-f",
                "-t", "zip",
                "-q",
                "-D", outputDir.path,
                "-j", "1",
                "-A", "cat > /dev/null; cat $cmcConlluAnnotation",
                baseZip
            )

            val exitCode = debug(args)
            assertEquals(0, exitCode, "External annotation processing should succeed")

            // Check that output ZIP has "cmc" in filename, not "annotated"
            val outputZip = File(outputDir, "wdd17sample.cmc.zip")
            assertTrue(outputZip.exists(), "Output ZIP should exist at ${outputZip.path} with 'cmc' foundry name")

            // Verify internal structure contains cmc folders
            val zipEntries = org.apache.commons.compress.archivers.zip.ZipFile.builder()
                .setFile(outputZip)
                .get()
                .use { zip ->
                    zip.entries.asSequence().map { it.name }.toList()
                }

            val cmcFolders = zipEntries.filter { it.contains("/cmc/") }
            assertTrue(cmcFolders.isNotEmpty(), "Should have cmc folders in output ZIP structure")

            // Verify no "annotated" folders exist
            val annotatedFolders = zipEntries.filter { it.contains("/annotated/") }
            assertTrue(annotatedFolders.isEmpty(), "Should NOT have 'annotated' folders in output ZIP structure")

            // Verify morpho.xml contains CMC annotations (EMOASC, EMOIMG, URL)
            val morphoXmlPath = "WDD17/B06/45592/cmc/morpho.xml"
            val morphoXml = org.apache.commons.compress.archivers.zip.ZipFile.builder()
                .setFile(outputZip)
                .get()
                .use { zip ->
                    val entry = zip.getEntry(morphoXmlPath)
                    assertNotNull(entry, "Should contain $morphoXmlPath")
                    zip.getInputStream(entry).bufferedReader(java.nio.charset.StandardCharsets.UTF_8).use { it.readText() }
                }

            assertTrue(morphoXml.contains("EMOASC"), "morpho.xml should contain EMOASC annotations")
            assertTrue(morphoXml.contains("EMOIMG"), "morpho.xml should contain EMOIMG annotations")
            assertTrue(morphoXml.contains("URL"), "morpho.xml should contain URL annotations")

        } finally {
            outputDir.deleteRecursively()
        }
    }
}
