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
}
