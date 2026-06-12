package de.ids_mannheim.korapxmltools

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Unit tests for the external-link derivation rules (issue #47). These are pure functions of the
 * header values, so they are tested directly without ZIP fixtures.
 */
class ExternalLinkResolverTest {

    @Test
    fun extractsBiblNoteId() {
        assertEquals("216199263", ExternalLinkResolver.biblNoteId("ID: 216199263 file: Originaldaten/2024/x@ Categories: ..."))
        assertEquals("A124349227", ExternalLinkResolver.biblNoteId("ID: A124349227"))
        assertNull(ExternalLinkResolver.biblNoteId("no id here"))
        assertNull(ExternalLinkResolver.biblNoteId(null))
    }

    @Test
    fun derivesWikipediaLinkFromReferenceText() {
        val reference = "WDD17/B06.45592 Diskussion:Berlinische Grammatik, In: Wikipedia - " +
            "URL:http://de.wikipedia.org/wiki/Diskussion:Berlinische_Grammatik: Wikipedia, 2017"
        val link = ExternalLinkResolver.wikipedia(reference)
        assertEquals("http://de.wikipedia.org/wiki/Diskussion:Berlinische_Grammatik", link?.url)
        assertEquals("Wikipedia", link?.title)
    }

    @Test
    fun ignoresNonWikipediaReference() {
        assertNull(ExternalLinkResolver.wikipedia("U24/JUN.00442 Süddeutsche Zeitung, 06.06.2024, S. 14"))
        assertNull(ExternalLinkResolver.wikipedia(null))
    }

    @Test
    fun derivesGeniosLinkFromSigleAndId() {
        // W24 -> botkuerzel "w" -> orikuerzel WELT
        val link = ExternalLinkResolver.genios("W24", "216199263")
        assertEquals("https://www.genios.de/document/WELT__216199263", link?.url)
        assertEquals("GENIOS", link?.title)
    }

    @Test
    fun returnsNoGeniosLinkForUnknownSigle() {
        assertNull(ExternalLinkResolver.genios("ZZ99", "123"))
        assertNull(ExternalLinkResolver.genios("W24", null))
    }

    @Test
    fun geniosRequiresWellFormedNewspaperSigle() {
        // "w" maps to WELT, but only a [A-Z]{1,3}[0-9]{2} sigle may trigger a Genios link.
        assertNull(ExternalLinkResolver.genios("W", "216199263"))             // no year volume
        assertNull(ExternalLinkResolver.genios("W2024", "216199263"))         // three digits
        assertNull(ExternalLinkResolver.genios("w24", "216199263"))           // lowercase
        assertNull(ExternalLinkResolver.genios("WELT24", "216199263"))        // four letters
        assertNull(ExternalLinkResolver.genios("W24/SEP.00359", "216199263")) // full text sigle, not corpus prefix
        // The well-formed sigle still works.
        assertEquals("https://www.genios.de/document/WELT__216199263", ExternalLinkResolver.genios("W24", "216199263")?.url)
    }

    @Test
    fun derivesSueddeutscheLinkFromSigleAndId() {
        val link = ExternalLinkResolver.sueddeutsche("U24", "A124349227")
        assertEquals("https://archiv.szarchiv.de/Portal/restricted/Start.act?articleId=A124349227", link?.url)
        assertEquals("Süddeutsche Zeitung", link?.title)
    }

    @Test
    fun sueddeutscheRequiresYearVolumeSigle() {
        // The two digits stand for the year volume, so a bare "U" must not match.
        assertNull(ExternalLinkResolver.sueddeutsche("U", "A1"))
        assertNull(ExternalLinkResolver.sueddeutsche("UXX", "A1"))
    }

    @Test
    fun derivesDgdLinkAndStripsTranscriptSuffix() {
        val link = ExternalLinkResolver.dgd("FOLK", "FOLK_E_00010_SE_01_DF_01")
        assertEquals(
            "https://dgd.ids-mannheim.de/DGD2Web/ExternalAccessServlet?command=displayData&id=FOLK_E_00010_SE_01",
            link?.url
        )
        assertEquals("DGD", link?.title)
        assertNull(ExternalLinkResolver.dgd("W24", "whatever"))
    }

    @Test
    fun resolvePrefersWikipediaOverIdBasedRules() {
        // A text that is both a Wikipedia reference and carries an ID + Genios sigle resolves to Wikipedia.
        val reference = "X URL:http://de.wikipedia.org/wiki/Foo: Wikipedia, 2017"
        val link = ExternalLinkResolver.resolve(
            corpusSigle = "W24", reference = reference, biblNoteId = "216199263", title = "Foo"
        )
        assertEquals("http://de.wikipedia.org/wiki/Foo", link?.url)
        assertEquals("Wikipedia", link?.title)
    }

    @Test
    fun resolvePrefersSueddeutscheOverGenios() {
        // U-sigle texts go to szarchiv even though "u" could collide with a Genios botkuerzel.
        val link = ExternalLinkResolver.resolve(
            corpusSigle = "U24", reference = null, biblNoteId = "A124349227", title = null
        )
        assertEquals("Süddeutsche Zeitung", link?.title)
    }

    @Test
    fun resolveReturnsNullWhenNothingMatches() {
        assertNull(ExternalLinkResolver.resolve("ZZZ", "plain reference", null, "title"))
    }
}
