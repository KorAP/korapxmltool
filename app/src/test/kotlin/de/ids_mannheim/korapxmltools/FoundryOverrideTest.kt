package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import kotlin.test.Ignore
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertFalse

class FoundryOverrideTest {
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

    private fun isDockerAvailable(): Boolean {
        return try {
            val process = ProcessBuilder("docker", "--version")
                .redirectErrorStream(true)
                .start()
            process.waitFor()
            process.exitValue() == 0
        } catch (e: Exception) {
            false
        }
    }
    
    @Test
    fun testFoundryOverrideWithTagger() {
        val isRunningInDocker = File("/.dockerenv").exists() || 
            (File("/proc/1/cgroup").exists() && File("/proc/1/cgroup").readText().contains("docker"))
        org.junit.Assume.assumeFalse("Skipping Docker test inside Docker container", isRunningInDocker)
        org.junit.Assume.assumeTrue("Docker is not available", isDockerAvailable())

        val outputDir = File.createTempFile("foundry_override_test", "").apply {
            delete()
            mkdirs()
        }
        try {
            val baseZip = loadResource("wud24_sample.zip").path
            val args = arrayOf(
                "-f",
                "-T", "spacy",
                "-q",
                "-D", outputDir.path,
                "-F", "xyz",
                "-t", "zip",
                baseZip
            )

            // Using debug() which calls KorapXmlTool().call()
            val exitCode = debug(args)
            assertEquals(0, exitCode, "Tool execution should succeed")

            // Check if the correct file exists
            val expectedFile = File(outputDir, "wud24_sample.xyz.zip")
            assertTrue(expectedFile.exists(), "Output file should be named wud24_sample.xyz.zip")

            // Check if the wrong file does NOT exist
            val wrongFile = File(outputDir, "wud24_sample.spacy.zip")
            assertFalse(wrongFile.exists(), "Output file wud24_sample.spacy.zip should NOT exist")

            // Check content of the zip
            val zipEntries = org.apache.commons.compress.archivers.zip.ZipFile.builder()
                .setFile(expectedFile)
                .get()
                .use { zip ->
                    zip.entries.asSequence().map { it.name }.toList()
                }

            assertTrue(zipEntries.any { it.contains("/xyz/morpho.xml") }, "Zip should contain entries with 'xyz' foundry")
            assertFalse(zipEntries.any { it.contains("/spacy/morpho.xml") }, "Zip should NOT contain entries with 'spacy' foundry")

        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun testOutputOptionHasPriority() {
        val outputDir = File.createTempFile("output_option_test", "").apply {
            delete()
            mkdirs()
        }
        try {
            val baseZip = loadResource("wud24_sample.zip").path
            val explicitOutputFile = File(outputDir, "my_custom_output.zip")
            val args = arrayOf(
                "-f",
                "-q",
                "-T", "spacy",
                "-o", explicitOutputFile.path,
                "-t", "zip",
                baseZip
            )

            val exitCode = debug(args)
            assertEquals(0, exitCode, "Tool execution should succeed")

            assertTrue(explicitOutputFile.exists(), "Explicit output file should exist: ${explicitOutputFile.path}")
            
            // Ensure the default-named file does NOT exist
            val defaultFile = File(outputDir, "wud24_sample.spacy.zip")
            assertFalse(defaultFile.exists(), "Default output file should NOT exist when -o is used")

        } finally {
            outputDir.deleteRecursively()
        }
    }
}
