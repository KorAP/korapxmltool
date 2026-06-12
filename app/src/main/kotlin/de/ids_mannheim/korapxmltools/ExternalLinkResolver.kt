package de.ids_mannheim.korapxmltools

/**
 * Derives a text's external full-text link for Krill output in cases where the link is not
 * explicitly encoded as a `ref[@type=page_url]`/`link` element, mirroring the behaviour of the
 * predecessor tool (KorAP-XML-Krill, KorAP::XML::Meta::I5):
 *
 *  - Wikipedia: the URL is only present in the `reference[type=complete]` text.
 *  - DGD/AGD/FOLK: a DGD access link is built from the corpus sigle and the transcript title.
 *  - Süddeutsche Zeitung (sigle `U<yy>`): an szarchiv link is built from the biblNote `ID:`.
 *  - Genios newspapers: a genios.de link is built from the corpus sigle (via [GeniosKuerzelMap])
 *    and the biblNote `ID:`.
 *
 * All rules are pure functions of the header values so they can be unit-tested without ZIP
 * fixtures. [resolve] applies them in priority order; the first match wins.
 */
object ExternalLinkResolver {
    data class ResolvedLink(val url: String, val title: String)

    // Wikipedia references end in "... URL:<url>: Wikipedia, <year>" (http or https).
    private val WIKIPEDIA_REFERENCE = Regex("""URL:(https?:.+?):\s+Wikipedia,\s+\d+\s*$""")
    // biblNote carries the source id as "ID: <id> ..." (id may itself start with a letter, e.g. SZ).
    private val BIBL_NOTE_ID = Regex("""ID:\s*(\S+)""")
    // Local newspaper corpus sigles always look like [A-Z]{1,3}[0-9]{2}; the two digits are the
    // year volume. Genios detection requires this shape so arbitrary sigles can't trigger a link.
    private val NEWSPAPER_SIGLE = Regex("""^[A-Z]{1,3}\d{2}$""")
    private val SUEDDEUTSCHE_SIGLE = Regex("""^U\d{2}$""")
    private val DGD_SIGLE = Regex("""^(?:[AD]GD|FOLK)$""")
    private val DGD_TRANSCRIPT_SUFFIX = Regex("""_DF_\d+$""", RegexOption.IGNORE_CASE)

    /** Extract the `ID:` value from a biblNote text, or null if absent. */
    fun biblNoteId(text: String?): String? =
        text?.let { BIBL_NOTE_ID.find(it)?.groupValues?.get(1) }

    /** The Genios botkuerzel for a corpus sigle: its leading letters, lowercased (e.g. W24 -> "w"). */
    private fun botkuerzel(corpusSigle: String?): String? =
        corpusSigle?.takeWhile { it.isLetter() }?.lowercase()?.takeIf { it.isNotEmpty() }

    fun wikipedia(reference: String?): ResolvedLink? =
        reference?.let { WIKIPEDIA_REFERENCE.find(it) }
            ?.let { ResolvedLink(it.groupValues[1], "Wikipedia") }

    fun dgd(corpusSigle: String?, title: String?): ResolvedLink? {
        if (corpusSigle == null || !DGD_SIGLE.matches(corpusSigle)) return null
        val transcript = title?.trim()?.takeIf { it.isNotEmpty() }
            ?.replace(DGD_TRANSCRIPT_SUFFIX, "") ?: return null
        return ResolvedLink(
            "https://dgd.ids-mannheim.de/DGD2Web/ExternalAccessServlet?command=displayData&id=$transcript",
            "DGD"
        )
    }

    fun sueddeutsche(corpusSigle: String?, biblNoteId: String?): ResolvedLink? {
        if (corpusSigle == null || !SUEDDEUTSCHE_SIGLE.matches(corpusSigle) || biblNoteId == null) return null
        return ResolvedLink(
            "https://archiv.szarchiv.de/Portal/restricted/Start.act?articleId=$biblNoteId",
            "Süddeutsche Zeitung"
        )
    }

    fun genios(corpusSigle: String?, biblNoteId: String?): ResolvedLink? {
        if (corpusSigle == null || biblNoteId == null || !NEWSPAPER_SIGLE.matches(corpusSigle)) return null
        val orikuerzel = botkuerzel(corpusSigle)?.let { GeniosKuerzelMap.byBotkuerzel[it] } ?: return null
        return ResolvedLink("https://www.genios.de/document/${orikuerzel}__$biblNoteId", "GENIOS")
    }

    /**
     * Try every derivation rule in priority order and return the first match. Wikipedia is
     * content-detected and wins over the sigle-based rules; Süddeutsche is checked before Genios
     * so `U<yy>` sigles resolve to szarchiv rather than a colliding Genios botkuerzel.
     */
    fun resolve(corpusSigle: String?, reference: String?, biblNoteId: String?, title: String?): ResolvedLink? =
        wikipedia(reference)
            ?: dgd(corpusSigle, title)
            ?: sueddeutsche(corpusSigle, biblNoteId)
            ?: genios(corpusSigle, biblNoteId)
}
