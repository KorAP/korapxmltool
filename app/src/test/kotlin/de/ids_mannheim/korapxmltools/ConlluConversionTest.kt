package de.ids_mannheim.korapxmltools

import org.apache.commons.compress.archivers.zip.ZipFile
import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import java.nio.charset.StandardCharsets
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
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
        }
        finally {
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
        return ZipFile.builder().setFile(zipFile).get().use { zip ->
            zip.entries.asSequence().map { it.name }.toList()
        }
    }

    private fun extractFileFromZip(zipFile: File, filePath: String): String {
        return ZipFile.builder().setFile(zipFile).get().use { zip ->
            val entry = zip.getEntry(filePath)
                ?: throw RuntimeException("Failed to find entry $filePath in $zipFile")
            zip.getInputStream(entry).bufferedReader(StandardCharsets.UTF_8).use { it.readText() }
        }
    }

    @Test
    fun testFoundrySpecificFilenameInConlluOutput() {
        // Regression test for issue where CoNLL-U output showed base/tokens.xml
        // instead of foundry-specific file (e.g., spacy/morpho.xml)
        val spacyZip = loadResource("wud24_sample.spacy.zip")
        val args = arrayOf("-t", "conllu", spacyZip.path)
        
        debug(args)
        
        val output = outContent.toString()
        val lines = output.split("\n")
        
        // Check that foundry is correctly identified
        val foundryLine = lines.find { it.startsWith("# foundry = ") }
        assertNotNull(foundryLine, "Should have foundry comment line")
        assertEquals("# foundry = spacy", foundryLine)
        
        // Check that filename points to spacy/morpho.xml, not base/tokens.xml
        val filenameLine = lines.find { it.startsWith("# filename = ") }
        assertNotNull(filenameLine, "Should have filename comment line")
        assertTrue(
            filenameLine.contains("/spacy/morpho.xml"),
            "Filename should point to spacy/morpho.xml, but was: $filenameLine"
        )
        assertFalse(
            filenameLine.contains("/base/tokens.xml"),
            "Filename should not point to base/tokens.xml, but was: $filenameLine"
        )
        
        // Verify the specific expected pattern for spacy foundry
        assertTrue(
            filenameLine.matches(Regex("# filename = .*/spacy/morpho\\.xml")),
            "Filename should match pattern '*/spacy/morpho.xml', but was: $filenameLine"
        )
    }
    @Test
    fun conlluPlusNamedEntitiesBecomeNeLayer() {
        val outputDir = createTempDir("conllup_ne")
        try {
            // Columns deliberately reordered: global.columns decides which field is which
            val conllup = File(outputDir, "mona_ner.conllup")
            conllup.writeText(
                "# global.columns = ID FORM NER LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC\n" +
                "# foundry = monapipe\n" +
                "# text_id = TG26-part1_Alberti-Konrad.00001\n" +
                "# start_offsets = 0 0 7 15\n" +
                "# end_offsets = 16 6 14 16\n" +
                "1\tKonrad\tB-PER\tKonrad\tPROPN\tNE\tCase=Nom\t_\t_\t_\t_\n" +
                "2\tAlberti\tI-PER\tAlberti\tPROPN\tNE\tCase=Nom\t_\t_\t_\t_\n" +
                "3\t-\t_\t-\tPUNCT\t\$(\t_\t_\t_\t_\t_\n\n"
            )

            val exitCode = debug(arrayOf("-t", "zip", "-D", outputDir.path, conllup.path))
            assertEquals(0, exitCode, "CoNLL-U Plus conversion should succeed")
            val outputZip = File(outputDir, "mona_ner.zip")
            assertTrue(outputZip.exists(), "Output ZIP should be auto-inferred as mona_ner.zip")

            val basePath = "TG26-part1/Alberti-Konrad/00001/monapipe"
            val morphoXml = extractFileFromZip(outputZip, "$basePath/morpho.xml")
            assertTrue(morphoXml.contains("<f name=\"lemma\">Alberti</f>"), "Lemma should be read from its declared column")
            assertTrue(morphoXml.contains("<f name=\"pos\">NE</f>"), "XPOS should be read from its declared column")
            assertFalse(morphoXml.contains("PER"), "NE tags must not leak into morpho.xml")

            val neXml = extractFileFromZip(outputZip, "$basePath/ne.xml")
            assertTrue(neXml.contains("type=\"ne\""), "ne.xml should use the CoreNLP ne feature structure")
            assertTrue(Regex("""from="0"[^>]*to="6"[^>]*>\s*<fs[^>]*>\s*<f name="ne">\s*<fs>\s*<f name="ent">B-PER</f>""")
                .containsMatchIn(neXml), "B-PER should be on span 0-6:\n$neXml")
            assertTrue(Regex("""from="7"[^>]*to="14"[\s\S]*?<f name="ent">I-PER</f>""").containsMatchIn(neXml),
                "I-PER should be on span 7-14:\n$neXml")
            assertEquals(2, Regex("<span ").findAll(neXml).count(), "Untagged tokens get no ne span")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    private fun convertConllup(outputDir: File, content: String): Pair<Int, File> {
        val conllup = File(outputDir, "input.conllup")
        conllup.writeText(content)
        val outputZip = File(outputDir, "input.zip")
        return debug(arrayOf("-t", "zip", "-o", outputZip.path, conllup.path)) to outputZip
    }

    private val konradHeader =
        "# foundry = monapipe\n" +
        "# text_id = TG26-part1_Alberti-Konrad.00001\n" +
        "# start_offsets = 0 0 7\n" +
        "# end_offsets = 14 6 14\n"

    @Test
    fun conlluPlusIdColumnIsResolvedFromHeader() {
        val outputDir = createTempDir("conllup_id_column")
        try {
            val (exitCode, outputZip) = convertConllup(outputDir,
                "# global.columns = FORM ID NER\n" + konradHeader +
                "Konrad\t1\tB-PER\nAlberti\t2\tI-PER\n\n")
            assertEquals(0, exitCode)
            val neXml = extractFileFromZip(outputZip, "TG26-part1/Alberti-Konrad/00001/monapipe/ne.xml")
            assertTrue(Regex("""from="0"[^>]*to="6"[\s\S]*?<f name="ent">B-PER</f>""").containsMatchIn(neXml),
                "Token 1 must be found via the ID column declared in global.columns:\n$neXml")
            assertTrue(Regex("""from="7"[^>]*to="14"[\s\S]*?<f name="ent">I-PER</f>""").containsMatchIn(neXml),
                "Token 2 must be found via the ID column declared in global.columns:\n$neXml")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun conlluPlusWithoutIdColumnIsRejected() {
        val outputDir = createTempDir("conllup_no_id")
        try {
            val (exitCode, _) = convertConllup(outputDir,
                "# global.columns = FORM NER\n" + konradHeader + "Konrad\tB-PER\nAlberti\tI-PER\n\n")
            assertTrue(exitCode != 0, "global.columns without ID must not silently produce an empty ZIP")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun conlluPlusRejectsSeveralNerColumns() {
        val outputDir = createTempDir("conllup_two_ner")
        try {
            // One NE layer per foundry: a second NE annotation belongs into its own foundry
            val (exitCode, outputZip) = convertConllup(outputDir,
                "# global.columns = ID FORM A:NER B:NER\n" + konradHeader +
                "1\tKonrad\tB-PER\tB-ORG\n2\tAlberti\t_\tI-ORG\n\n")
            assertTrue(exitCode != 0, "Several NER columns must be rejected, not silently merged or dropped")
            assertFalse(outputZip.exists(), "No output ZIP should be written")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun conlluPlusKeepsNerAlternativesInGivenOrder() {
        val outputDir = createTempDir("conllup_ner_alternatives")
        try {
            // Alternatives within one column work like POS alternatives (ADJD|ADV)
            val (exitCode, outputZip) = convertConllup(outputDir,
                "# global.columns = ID FORM NER\n" + konradHeader +
                "1\tKonrad\tB-PER|B-ORG|B-PER\n2\tAlberti\tI-PER\n\n")
            assertEquals(0, exitCode)
            val neXml = extractFileFromZip(outputZip, "TG26-part1/Alberti-Konrad/00001/monapipe/ne.xml")
            val konrad = neXml.substringAfter("from=\"0\"").substringBefore("</span>")
            assertEquals(listOf("B-PER", "B-ORG"),
                Regex("<f name=\"ent\">([^<]+)</f>").findAll(konrad).map { it.groupValues[1] }.toList(),
                "Each alternative gets its own feature structure, in the given order, without duplicates:\n$neXml")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun conlluWithoutNeColumnWritesNoNeLayer() {
        val outputDir = createTempDir("conllu_no_ne")
        try {
            val outputZip = File(outputDir, "output.zip")
            debug(arrayOf("-t", "zip", "-o", outputZip.path, loadResource("wud24_sample.spacy.conllu").path))
            assertFalse(extractZipFileList(outputZip).any { it.endsWith("/ne.xml") },
                "Plain CoNLL-U has no NE column and should not produce ne.xml")
        } finally {
            outputDir.deleteRecursively()
        }
    }

    @Test
    fun sparseAnnotationRespectsTokenIds() {
        val outputDir = createTempDir("conllu_sparse")
        try {
            // Create CoNLL-U with sparse annotation (only for token 7)
            val sparseConllu = File(outputDir, "sparse.conllu")
            sparseConllu.writeText("""
                # foundry = cmc
                # filename = NDY/115/005255/base/tokens.xml
                # text_id = NDY_115.005255
                # start_offsets = 0 0 4 11 18 22 27 32 35 41 46 50 56 64
                # end_offsets = 65 3 10 17 21 26 31 34 40 45 49 55 64 65
                7	:)	_	_	EMOASC	_	_	_	_	_
                
            """.trimIndent())
            
            val outputZip = File(outputDir, "output.zip")
            val args = arrayOf(
                "-t", "zip",
                "-o", outputZip.path,
                sparseConllu.path
            )
            val exitCode = debug(args)
            assertEquals(0, exitCode, "Sparse conversion should succeed")
            
            // Extract morpho.xml
            val morphoXml = extractFileFromZip(outputZip, "NDY/115/005255/cmc/morpho.xml")
            
            // Verify that the annotation is on the correct span (32-34)
            // Offset for ID 7 is start=32 (index 7), end=34 (index 7)
            // Note: Attribute order is not guaranteed, so check for attributes individually
            assertTrue(
                morphoXml.contains("""from="32"""") && morphoXml.contains("""to="34""""),
                "Annotation should be on span 32-34 (ID 7), but morpho.xml content was:\n$morphoXml"
            )
            
            // Verify the content of the annotation
            assertTrue(morphoXml.contains(">EMOASC<"), "Should contain the annotation EMOASC")
        } finally {
            outputDir.deleteRecursively()
        }
    }
}
