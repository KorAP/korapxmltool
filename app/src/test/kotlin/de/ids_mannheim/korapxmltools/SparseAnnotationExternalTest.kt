package de.ids_mannheim.korapxmltools

import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
import org.junit.Test
import java.io.File
import java.io.FileOutputStream
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SparseAnnotationExternalTest {

    private fun extractFileFromZip(zipFile: File, regex: Regex): String? {
        val zip = org.apache.commons.compress.archivers.zip.ZipFile(zipFile)
        val entry = zip.entries.asSequence().firstOrNull { regex.matches(it.name) }
        return if (entry != null) {
            zip.getInputStream(entry).bufferedReader().use { it.readText() }
        } else {
            null
        }
    }

    @Test
    fun sparseAnnotationRespectsTokenIdsWithExternalTool() {
        val outputDir = createTempDir("conllu_sparse_external")
        try {
            val outputZip = File(outputDir, "output.zip")
            val tool = KorapXmlTool()
            
            // Setup internal state
            tool.morphoZipOutputStream = ZipArchiveOutputStream(FileOutputStream(outputZip))
            tool.tokenSeparator = "\n"
            
            // Create a fake task
            val task = AnnotationWorkerPool.AnnotationTask(
                text = "", // Not used in parseAndWriteAnnotatedConllu logic concerning parsing content
                docId = "NDY_115.005255", 
                entryPath = "NDY/115/005255|cmc" // path|foundry
            )
            
            // Valid sparse CONLL-U content (same as in ConlluConversionTest)
            // Token 7 (index 6 0-based, or if using offsets directly index 7 in offset list list)
            // Offsets list has 14 items.
            // ID 7: 32-34
            val annotatedConllu = """
                # foundry = cmc
                # filename = NDY/115/005255/base/tokens.xml
                # text_id = NDY_115.005255
                # start_offsets = 0 0 4 11 18 22 27 32 35 41 46 50 56 64
                # end_offsets = 65 3 10 17 21 26 31 34 40 45 49 55 64 65
                7	:)	_	_	EMOASC	_	_	_	_	_
                
            """.trimIndent()

            // Invoke the internal method
            tool.parseAndWriteAnnotatedConllu(annotatedConllu, task)
            
            // Close stream to flush to disk
            tool.morphoZipOutputStream?.close()
            
            // Extract morpho.xml
            // Path structure: NDY/115/005255/cmc/morpho.xml
            val morphoXml = extractFileFromZip(outputZip, Regex(".*cmc/morpho.xml"))
            
            assertTrue(morphoXml != null, "morpho.xml should exist locally")
            
            // Verify that the annotation is on the correct span (32-34)
            // Note: Attribute order is not guaranteed, so check for attributes individually
            assertTrue(
                morphoXml!!.contains("""from="32"""") && morphoXml.contains("""to="34""""),
                "Annotation should be on span 32-34 (ID 7), but morpho.xml content was:\n$morphoXml"
            )
            
            // Verify the content of the annotation
            assertTrue(morphoXml.contains(">EMOASC<"), "Should contain the annotation EMOASC")
        } finally {
            outputDir.deleteRecursively()
        }
    }
    
    // Helper since kotlin-test doesn't strictly have createTempDir anymore in some versions or usually io.tmp
    private fun createTempDir(prefix: String): File {
        val f = java.nio.file.Files.createTempDirectory(prefix).toFile()
        f.deleteOnExit()
        return f
    }
}
