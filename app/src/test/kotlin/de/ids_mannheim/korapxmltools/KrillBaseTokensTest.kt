package de.ids_mannheim.korapxmltools

import org.apache.commons.compress.archivers.zip.ZipFile
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

class KrillBaseTokensTest {

    private fun openResource(name: String): Pair<ZipFile, String> {
        val path = File(Thread.currentThread().contextClassLoader.getResource(name)!!.toURI()).path
        return ZipFile.builder().setFile(path).get() to path
    }

    @Test
    fun tokenLayersOfAnnotationZipsDoNotReplaceBaseTokens() {
        val tool = KorapXmlTool().apply { outputFormat = OutputFormat.KRILL }
        val (base, basePath) = openResource("wud24_sample.zip")
        val (corenlp, corenlpPath) = openResource("wud24_sample.corenlp.zip")
        base.use { corenlp.use {
            val text = "WUD24/Z0087/65594"
            // 23:50 is one token in corenlp/tokens.xml, but two in base/tokens.xml. Entries are
            // processed in parallel; this is the order in which the wrong tokens used to win.
            tool.processZipEntry(base, basePath, "base", base.getEntry("$text/data.xml"), false)
            tool.processZipEntry(base, basePath, "base", base.getEntry("$text/base/tokens.xml"), false)
            tool.processZipEntry(corenlp, corenlpPath, "corenlp", corenlp.getEntry("$text/corenlp/tokens.xml"), false)
            tool.processZipEntry(base, basePath, "base", base.getEntry("$text/struct/structure.xml"), false)
        } }

        val data = tool.krillData.getValue("WUD24_Z0087.65594")
        val spans = data.tokens!!.map { it.from to it.to }
        assertEquals(listOf(1918 to 1920, 1920 to 1921, 1921 to 1923), spans.filter { it.first in 1918..1922 })
        assertEquals("base#tokens", data.headerMetadata["tokenSource"])
    }
}
