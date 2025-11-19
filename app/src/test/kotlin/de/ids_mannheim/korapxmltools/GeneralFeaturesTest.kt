package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.net.URL
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Tests for general tool features: help, logging, annotation, metadata, sorting
 */
class GeneralFeaturesTest {
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
    fun canPrintHelp() {
        debug(arrayOf("-h"))
        assertContains(outContent.toString(), "--s-bounds-from-morpho")
    }

    @Test
    fun canSetLogLevel() {
        val args = arrayOf("-l", "info", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(errContent.toString(), "Processing zip file")
    }

    @Test
    fun canAnnotate() {
        val args = arrayOf("-A", "sed -e 's/u/x/g'", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(outContent.toString(), "axtomatiqxe")
        assertTrue(
            "Annotated CoNLL-U should have at least as many lines as the original, but only has ${
                outContent.toString().count { it == '\n' }
            } lines"
        ) { outContent.toString().count { it == '\n' } >= 61511 }
    }

    @Test
    fun monthAwareComparatorOrdersCalendarMonths() {
        val tool = KorapXmlTool()
        assertTrue(tool.compareTextIds("ZGE24_JAN.00001", "ZGE24_MAR.00001") < 0, "JAN should sort before MAR")
        assertTrue(tool.compareTextIds("ZGE24_MRZ.00001", "ZGE24_APR.00001") < 0, "MRZ should sort before APR")
        assertTrue(tool.compareTextIds("ZGE24_OKT.00001", "ZGE24_SEP.00001") > 0, "OKT should sort after SEP")
        assertTrue(tool.compareTextIds("ZGE24_DEZ.00001", "ZGE24_NOV.00001") > 0, "DEZ should sort after NOV")
        assertTrue(tool.compareTextIds("ZGE24_MAI.00001", "ZGE24_JUL.00001") < 0, "MAI should sort before JUL")
    }

    @Test
    fun monthAwareComparatorFallsBackToAlphabeticalWhenNoMonth() {
        val tool = KorapXmlTool()
        val ids = listOf("WUD24_I0083.95367", "WUD24_Z0087.65594", "WUD24_K0086.98010")
        val sorted = ids.sortedWith { a, b -> tool.compareTextIds(a, b) }
        assertEquals(
            listOf("WUD24_I0083.95367", "WUD24_K0086.98010", "WUD24_Z0087.65594"),
            sorted,
            "Non-month IDs should sort alphabetically"
        )
    }

    @Test
    fun monthAwareComparatorSortsMixedMonthsInCalendarOrder() {
        val tool = KorapXmlTool()
        val ids = listOf(
            "ZGE24_OKT.00002",
            "ZGE24_JAN.00003",
            "ZGE24_DEZ.00001",
            "ZGE24_SEP.00005",
            "ZGE24_MAR.00001"
        )
        val expected = listOf(
            "ZGE24_JAN.00003",
            "ZGE24_MAR.00001",
            "ZGE24_SEP.00005",
            "ZGE24_OKT.00002",
            "ZGE24_DEZ.00001"
        )
        val sorted = ids.sortedWith { a, b -> tool.compareTextIds(a, b) }
        assertEquals(expected, sorted, "Mixed month IDs should follow calendar order")
    }

    private fun KorapXmlTool.compareTextIds(a: String, b: String): Int {
        val m = KorapXmlTool::class.java.getDeclaredMethod("compareTextIds", String::class.java, String::class.java)
        m.isAccessible = true
        return m.invoke(this, a, b) as Int
    }
}
