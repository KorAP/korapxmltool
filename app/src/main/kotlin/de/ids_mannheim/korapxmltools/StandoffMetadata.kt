package de.ids_mannheim.korapxmltools

import de.ids_mannheim.korapxmltools.formatters.KrillJsonGenerator.StandoffField
import org.w3c.dom.Element
import java.io.File
import java.util.logging.Logger
import javax.xml.parsers.DocumentBuilderFactory

/**
 * Reads stand-off metadata XML files (one file per corpus, in the `<standOff>`
 * form produced by e.g. the wiki-taxonomy classifier) and turns them into
 * ready-to-emit Krill metadata fields keyed by the text's docid.
 *
 * The format is TEI-namespaced and reuses TEI's classification vocabulary
 * (taxonomy/category/catRef), but is not conformant TEI P5: `standOff`,
 * `metadataLayer`, `textRef` and `source` are local extensions.
 *
 * Layout:
 *
 *     <standOff>
 *       <metadataLayer xml:id="wikiDomain" type="classification">
 *         <taxonomy .../>                               (declaration, ignored here)
 *         <textRef target="REI_RBR.00473">              (target == raw_text/@docid)
 *           <catRef target="#Language" n="1" cert="0.39"/>
 *         </textRef>
 *       </metadataLayer>
 *     </standOff>
 *
 * Selection policy for classification layers: keep the highest-ranked categories
 * whose `cert` is at least [classificationMinCert], capped at [classificationTopN],
 * and emit them as a single `type:keywords` field named after the layer's id.
 *
 * By default we ingest *everything* present in the file: the annotation tool that
 * produced it has already applied its own top-k/threshold, and we want to be able
 * to re-use existing classifications as-is without redoing them. The threshold
 * knobs below are kept so a future CLI option can re-enable curation at index time
 * (e.g. top 2 with cert >= 0.3).
 */
object StandoffMetadata {
    private val LOGGER = Logger.getLogger(StandoffMetadata::class.java.name)

    /** Default indexing selection: take all categories present in the file. */
    const val DEFAULT_TOP_N = Int.MAX_VALUE
    const val DEFAULT_MIN_CERT = 0.0

    private val factory: DocumentBuilderFactory by lazy {
        DocumentBuilderFactory.newInstance().apply {
            isNamespaceAware = true
        }
    }

    /**
     * Cheap recogniser used to auto-detect stand-off metadata among the input
     * files (so no dedicated CLI option is needed). True when [path] is an `.xml`
     * file whose root element is `standOff`.
     */
    fun isStandoffMetadataFile(path: String): Boolean {
        if (!path.endsWith(".xml", ignoreCase = true)) return false
        val file = File(path)
        if (!file.isFile) return false
        return try {
            val root = factory.newDocumentBuilder().parse(file).documentElement
            root.localName == "standOff" || root.tagName == "standOff"
        } catch (e: Exception) {
            LOGGER.fine("Not a stand-off metadata file ($path): ${e.message}")
            false
        }
    }

    /**
     * Parse [path] and merge its per-text fields into [target] (keyed by docid).
     */
    fun parseInto(
        path: String,
        target: MutableMap<String, MutableList<StandoffField>>,
        classificationTopN: Int = DEFAULT_TOP_N,
        classificationMinCert: Double = DEFAULT_MIN_CERT
    ) {
        val root = factory.newDocumentBuilder().parse(File(path)).documentElement
        var layers = 0
        var entries = 0
        root.childElements("metadataLayer").forEach { layer ->
            layers++
            val key = layer.idOrKey()
            if (key.isNullOrBlank()) {
                LOGGER.warning("Stand-off metadata layer without xml:id/key in $path; skipping")
                return@forEach
            }
            val type = layer.getAttribute("type").ifBlank { "classification" }
            layer.childElements("textRef").forEach { textRef ->
                val docId = textRef.getAttribute("target").removePrefix("#")
                if (docId.isBlank()) return@forEach
                val field = when (type) {
                    "classification" -> classificationField(key, textRef, classificationTopN, classificationMinCert)
                    "links" -> linkField(key, textRef)
                    else -> {
                        LOGGER.warning("Unknown stand-off metadata layer type '$type' in $path; skipping")
                        null
                    }
                } ?: return@forEach
                target.getOrPut(docId) { mutableListOf() }.add(field)
                entries++
            }
        }
        LOGGER.info("Loaded stand-off metadata from $path: $layers layer(s), $entries text entr(ies)")
    }

    private fun classificationField(
        key: String,
        textRef: Element,
        topN: Int,
        minCert: Double
    ): StandoffField? {
        val cats = textRef.childElements("catRef")
            .mapNotNull { catRef ->
                val target = catRef.getAttribute("target").removePrefix("#")
                if (target.isBlank()) return@mapNotNull null
                val cert = catRef.getAttribute("cert").toDoubleOrNull() ?: Double.NaN
                val rank = catRef.getAttribute("n").toIntOrNull() ?: Int.MAX_VALUE
                Triple(target, cert, rank)
            }
            .filter { it.second.isNaN() || it.second >= minCert }
            .sortedBy { it.third }
            .take(topN)
            .map { it.first }
        return if (cats.isEmpty()) null else StandoffField(key, "type:keywords", cats)
    }

    private fun linkField(key: String, textRef: Element): StandoffField? {
        // Emit the first <ref> as a KorAP link attachment, matching the encoding
        // used for textExternalLink in KrillJsonGenerator.
        val ref = textRef.childElements("ref").firstOrNull() ?: return null
        val url = ref.getAttribute("target")
        if (url.isBlank()) return null
        val title = ref.textContent?.trim()?.ifBlank { null } ?: "Link"
        val encodedUrl = url.replace(":", "%3A").replace("/", "%2F")
        return StandoffField(key, "type:attachement", "data:application/x.korap-link;title=$title,$encodedUrl")
    }

    private fun Element.idOrKey(): String? {
        val id = getAttributeNS("http://www.w3.org/XML/1998/namespace", "id")
        if (id.isNotBlank()) return id
        val xmlId = getAttribute("xml:id")
        if (xmlId.isNotBlank()) return xmlId
        return getAttribute("key").ifBlank { null }
    }

    private fun Element.childElements(localName: String): List<Element> {
        val result = mutableListOf<Element>()
        val children = childNodes
        for (i in 0 until children.length) {
            val node = children.item(i)
            if (node is Element && (node.localName == localName || node.tagName == localName)) {
                result.add(node)
            }
        }
        return result
    }

    /**
     * Parse the I5 `<xenoData>` block of a header element into ready-to-emit Krill
     * fields, mirroring KorAP-XML-Krill's `KorAP::XML::Meta::I5` xenodata handling.
     *
     * Each `<meta name="..." type="..." [project="..."] [desc="..."]>VALUE</meta>`
     * becomes one Krill field. The meta `type` selects the field type:
     *   string -> type:string, keyword -> type:keywords, text -> type:text,
     *   date -> type:date, attachment -> type:attachement, uri -> type:attachement
     *   (value wrapped as a korap-link data URI). Unknown types are skipped.
     *
     * The field key is `<project>.<name>` (the `<project>.` part only when a
     * project attribute is present). For doc/corpus headers the meta name is
     * prefixed with the level ("doc"/"corpus") and upper-cased, so the same meta
     * name stays a distinct field across levels (text `rcpnt`, doc `docRcpnt`,
     * corpus `corpusRcpnt`). Repeated keyword metas with the same key accumulate
     * into a list; repeated scalars keep the last value.
     *
     * @param header     the idsHeader root element (may be null)
     * @param headerType "text", "doc" or "corpus"
     */
    fun parseXenoData(header: Element?, headerType: String): List<StandoffField> {
        if (header == null) return emptyList()
        val xeno = header.getElementsByTagName("xenoData")
            .let { if (it.length == 0) null else it.item(0) as? Element }
            ?: return emptyList()

        // Keyed by the internal (type-prefixed) key so metas only collide when they
        // would in Perl, too; LinkedHashMap preserves document order of first sight.
        val ordered = LinkedHashMap<String, XenoEntry>()
        val metas = xeno.getElementsByTagName("meta")
        for (i in 0 until metas.length) {
            val meta = metas.item(i) as? Element ?: continue
            val name = meta.getAttribute("name").trim()
            if (name.isEmpty()) continue
            val value = squish(meta.textContent ?: "")
            if (value.isEmpty()) continue
            val xtype = meta.getAttribute("type").trim()
            if (xtype.isEmpty()) continue

            val typePrefix = when (xtype) {
                "string" -> "S_"
                "keyword" -> "K_"
                "text" -> "T_"
                "date" -> "D_"
                "attachment", "uri" -> "A_"
                else -> {
                    LOGGER.warning("Unknown xenodata type: $xtype")
                    continue
                }
            }
            val fieldType = when (xtype) {
                "string" -> "type:string"
                "keyword" -> "type:keywords"
                "text" -> "type:text"
                "date" -> "type:date"
                else -> "type:attachement" // attachment, uri
            }
            val fieldValue = when (xtype) {
                "uri" -> korapDataUri(value, meta.getAttribute("desc").trim().ifBlank { value })
                "attachment" -> if (value.startsWith("data:")) value else "data:,$value"
                "date" -> normalizeXenoDate(value)
                else -> value
            }

            val project = meta.getAttribute("project").trim()
            val nameKey = if (headerType == "doc" || headerType == "corpus") {
                headerType + name.replaceFirstChar { it.uppercase() }
            } else {
                name
            }
            val internalKey = typePrefix + (if (project.isNotEmpty()) "$project." else "") + nameKey

            val entry = ordered.getOrPut(internalKey) { XenoEntry(fieldKey(internalKey), fieldType) }
            if (xtype == "keyword") {
                entry.keywords.add(fieldValue)
            } else {
                entry.scalar = fieldValue
            }
        }

        return ordered.values.map { entry ->
            if (entry.type == "type:keywords") {
                StandoffField(entry.key, entry.type, entry.keywords.toList())
            } else {
                StandoffField(entry.key, entry.type, entry.scalar ?: "")
            }
        }
    }

    private class XenoEntry(val key: String, val type: String) {
        val keywords = mutableListOf<String>()
        var scalar: String? = null
    }

    /**
     * Derive the Krill field key from the internal type-prefixed key, mirroring
     * KorAP::XML::Meta::Base::_k: strip the 2-char type prefix, camel-case `_x`
     * sequences and upper-case a trailing "id".
     */
    private fun fieldKey(internalKey: String): String {
        var x = internalKey.substring(2)
        x = Regex("_(\\w)").replace(x) { it.groupValues[1].uppercase() }
        x = x.replace(Regex("(?i)id$"), "ID")
        return x
    }

    /** Collapse internal whitespace runs and trim; an all-dashes value becomes empty. */
    private fun squish(s: String): String {
        var v = s.replace(Regex("\\s\\s+"), " ").trim()
        if (v.matches(Regex("^-+$"))) v = ""
        return v
    }

    /** Normalise an 8-digit YYYYMMDD date to YYYY[-MM[-DD]]; pass other forms through. */
    private fun normalizeXenoDate(value: String): String {
        val m = Regex("^(\\d{4})(\\d{2})(\\d{2})$").find(value) ?: return value
        val (y, mo, d) = m.destructured
        val sb = StringBuilder(y)
        if (mo != "00") {
            sb.append("-").append(mo)
            if (d != "00") sb.append("-").append(d)
        }
        return sb.toString()
    }

    /** Build a `data:application/x.korap-link` URI, mirroring Meta::Base::korap_data_uri. */
    private fun korapDataUri(data: String, title: String): String =
        "data:application/x.korap-link;title=${urlEscape(title)},${urlEscape(data)}"

    private fun urlEscape(s: String): String =
        java.net.URLEncoder.encode(s, "UTF-8").replace("+", "%20")
}
