package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Tests for CoNLL-U to KorAP XML ZIP conversion functionality
 */
class ConlluConversionTest {
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

    private fun createTempDir(prefix: String): File {
        return File.createTempFile(prefix, "").apply {
            delete()
            mkdirs()
        }
    }

    @Test
    fun canConvertBasicConlluToZip() {
        val outputDir = createTempDir("conllu_basic")
        try {
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path,
                loadResource("wud24_sample.spacy.conllu").path
            )
            val exitCode = debug(args)
            assertEquals(0, exitCode, "CoNLL-U conversion should succeed")
            assertTrue(outputZip.exists(), "Output ZIP should be created")
            assertTrue(outputZip.length() > 0, "Output ZIP should not be empty")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun conlluZipContainsMorphoXml() {
        val outputDir = createTempDir("conllu_morpho")
        try {
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path,
                loadResource("wud24_sample.spacy.conllu").path
            )
            debug(args)
            
            val zipEntries = extractZipFileList(outputZip)
            assertTrue(zipEntries.any { it.contains("spacy/morpho.xml") }, 
                "ZIP should contain morpho.xml files")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun conlluZipContainsDependencyXml() {
        val outputDir = createTempDir("conllu_dependency")
        try {
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path,
                loadResource("wud24_sample.spacy.conllu").path
            )
            debug(args)
            
            val zipEntries = extractZipFileList(outputZip)
            assertTrue(zipEntries.any { it.contains("spacy/dependency.xml") }, 
                "ZIP should contain dependency.xml files when dependencies present")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun canAutoInferOutputFilename() {
        val outputDir = createTempDir("conllu_autoinfer")
        try {
            // Copy test file to temp dir
            val inputFile = File(outputDir, "test.conllu")
            File(loadResource("wud24_sample.spacy.conllu").path).copyTo(inputFile)
            
            val args = arrayOf(
                "-t", "zip",
                "-D", outputDir.path,
                inputFile.path
            )
            val exitCode = debug(args)
            assertEquals(0, exitCode, "CoNLL-U conversion should succeed")
            
            val outputZip = File(outputDir, "test.zip")
            assertTrue(outputZip.exists(), "Output ZIP should be auto-inferred as test.zip")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun respectsOutputDirOption() {
        val outputDir = createTempDir("conllu_output_dir")
        try {
            val inputFile = File(outputDir, "input.conllu")
            File(loadResource("wud24_sample.spacy.conllu").path).copyTo(inputFile)
            
            val args = arrayOf(
                "-t", "zip",
                "-D", outputDir.path,
                inputFile.path
            )
            debug(args)
            
            val outputZip = File(outputDir, "input.zip")
            assertTrue(outputZip.exists(), "Output should be in specified directory")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun canHandleCombinedFoundries() {
        val outputDir = createTempDir("conllu_combined")
        try {
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path,
                loadResource("wud24_sample.marmot-malt.conllu").path
            )
            val exitCode = debug(args)
            assertEquals(0, exitCode, "Combined foundry conversion should succeed")
            
            val zipEntries = extractZipFileList(outputZip)
            assertTrue(zipEntries.any { it.contains("marmot/morpho.xml") }, 
                "ZIP should contain marmot morpho.xml")
            assertTrue(zipEntries.any { it.contains("malt/dependency.xml") }, 
                "ZIP should contain malt dependency.xml")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun canOverrideFoundryName() {
        val outputDir = createTempDir("conllu_override")
        try {
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-F", "custom",
                "-o", outputZip.path,
                loadResource("wud24_sample.spacy.conllu").path
            )
            val exitCode = debug(args)
            assertEquals(0, exitCode, "Foundry override conversion should succeed")
            
            val zipEntries = extractZipFileList(outputZip)
            assertTrue(zipEntries.any { it.contains("custom/morpho.xml") }, 
                "ZIP should contain custom foundry morpho.xml")
            assertFalse(zipEntries.any { it.contains("spacy/morpho.xml") }, 
                "ZIP should not contain original spacy foundry")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun canConvertFromStdin() {
        val outputDir = createTempDir("conllu_stdin")
        try {
            val outputZip = File(outputDir, "stdin_output.zip")
            val inputFile = File(loadResource("wud24_sample.spacy.conllu").path)
            
            // Use KorapXmlTool directly with redirected stdin
            val inputStream = inputFile.inputStream()
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path
            )
            
            val originalIn = System.`in`
            try {
                System.setIn(inputStream)
                val exitCode = debug(args)
                assertEquals(0, exitCode, "Stdin conversion should succeed")
                assertTrue(outputZip.exists(), "Output ZIP should be created from stdin")
            } finally {
                System.setIn(originalIn)
                inputStream.close()
            }
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun validatesRequiredTextId() {
        val outputDir = createTempDir("conllu_validation")
        try {
            // Create invalid CoNLL-U without text_id
            val invalidConllu = File(outputDir, "invalid.conllu")
            invalidConllu.writeText("""
                # foundry = test
                # start_offsets = 0 5
                # end_offsets = 4 10
                1	Test	test	NOUN	NN	_	0	ROOT	_	_
                
            """.trimIndent())
            
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path,
                invalidConllu.path
            )
            val exitCode = debug(args)
            assertTrue(exitCode != 0, "Should fail without text_id")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun handlesMultipleDocuments() {
        val outputDir = createTempDir("conllu_multidoc")
        try {
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path,
                loadResource("wud24_sample.spacy.conllu").path
            )
            val exitCode = debug(args)
            assertEquals(0, exitCode, "Multi-document conversion should succeed")
            
            val zipEntries = extractZipFileList(outputZip)
            // The sample file has 3 documents: WUD24_I0083.95367, WUD24_K0086.98010, WUD24_Z0087.65594
            assertTrue(zipEntries.any { it.contains("WUD24/I0083/95367") }, 
                "Should contain first document")
            assertTrue(zipEntries.any { it.contains("WUD24/K0086/98010") }, 
                "Should contain second document")
            assertTrue(zipEntries.any { it.contains("WUD24/Z0087/65594") }, 
                "Should contain third document")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun createsValidKorapXmlStructure() {
        val outputDir = createTempDir("conllu_xml_validation")
        try {
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path,
                loadResource("wud24_sample.spacy.conllu").path
            )
            val exitCode = debug(args)
            assertEquals(0, exitCode)
            
            // Check ZIP contains expected files
            val zipEntries = extractZipFileList(outputZip)
            assertTrue(zipEntries.any { it.contains("WUD24/I0083/95367/spacy/morpho.xml") },
                "ZIP should contain morpho.xml")
            assertTrue(zipEntries.any { it.contains("WUD24/I0083/95367/spacy/dependency.xml") },
                "ZIP should contain dependency.xml")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun morphoXmlContainsLexicalFeatures() {
        val outputDir = createTempDir("conllu_morpho_features")
        try {
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path,
                loadResource("wud24_sample.spacy.conllu").path
            )
            debug(args)
            
            val morphoXml = extractFileFromZip(outputZip, "WUD24/I0083/95367/spacy/morpho.xml")
            assertTrue(morphoXml.length > 100, "Morpho XML should have substantial content")
            assertTrue(morphoXml.contains("lemma") && morphoXml.contains("upos"),
                "Morpho XML should contain lemma and upos fields")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun dependencyXmlContainsDependencyRelations() {
        val outputDir = createTempDir("conllu_dep_relations")
        try {
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path,
                loadResource("wud24_sample.spacy.conllu").path
            )
            debug(args)
            
            val depXml = extractFileFromZip(outputZip, "WUD24/I0083/95367/spacy/dependency.xml")
            assertTrue(depXml.length > 100, "Dependency XML should have substantial content")
            assertTrue(depXml.contains("deprel") || depXml.contains("label"),
                "Dependency XML should contain dependency relations")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun xmlContainsCorrectDocumentId() {
        val outputDir = createTempDir("conllu_docid")
        try {
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path,
                loadResource("wud24_sample.spacy.conllu").path
            )
            debug(args)
            
            val morphoXml = extractFileFromZip(outputZip, "WUD24/I0083/95367/spacy/morpho.xml")
            assertTrue(morphoXml.contains("WUD24_I0083.95367"),
                "XML should contain correct document ID")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    private fun extractZipFileList(zipFile: File): List<String> {
        val process = ProcessBuilder("unzip", "-l", zipFile.path)
            .redirectOutput(ProcessBuilder.Redirect.PIPE)
            .start()
        val output = process.inputStream.bufferedReader().use { it.readText() }
        process.waitFor()
        return output.lines()
    }

    private fun extractFileFromZip(zipFile: File, filePath: String): String {
        val process = ProcessBuilder("unzip", "-p", zipFile.path, filePath)
            .redirectOutput(ProcessBuilder.Redirect.PIPE)
            .redirectError(ProcessBuilder.Redirect.PIPE)
            .start()
        val content = process.inputStream.bufferedReader().use { it.readText() }
        val exitCode = process.waitFor()
        if (exitCode != 0) {
            val error = process.errorStream.bufferedReader().use { it.readText() }
            throw RuntimeException("Failed to extract $filePath from $zipFile: $error")
        }
        return content
    }
}
