package de.ids_mannheim.korapxmltools.formatters

import de.ids_mannheim.korapxmltools.ConstituencyParserBridge
import org.w3c.dom.Document
import java.io.StringWriter
import java.util.logging.Logger
import javax.xml.parsers.DocumentBuilder
import javax.xml.transform.OutputKeys
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult

/**
 * Formatter for KorAP-XML output format.
 * Generates XML layers with morphological, dependency, or constituency annotations.
 */
object KorapXmlFormatter : OutputFormatter {
    private val LOGGER = Logger.getLogger(KorapXmlFormatter::class.java.name)

    override val formatName: String = "korapxml"

    override fun format(context: OutputContext): StringBuilder {
        // Requires a DocumentBuilder to be passed
        val dBuilder = context.documentBuilder
            ?: throw IllegalArgumentException("DocumentBuilder required for KorAP-XML output")
        
        val parserName = context.parserName
        val constituencyParserName = context.constituencyParserName
        
        return when {
            constituencyParserName != null -> formatConstituency(context, dBuilder)
            parserName != null -> formatDependency(context, dBuilder)
            else -> formatMorpho(context, dBuilder)
        }
    }

    /**
     * Format morphological annotations as KorAP-XML.
     */
    fun formatMorpho(context: OutputContext, dBuilder: DocumentBuilder): StringBuilder {
        val doc: Document = dBuilder.newDocument()

        // Root element
        val layer = doc.createElement("layer")
        layer.setAttribute("xmlns", "http://ids-mannheim.de/ns/KorAP")
        layer.setAttribute("version", "KorAP-0.4")
        layer.setAttribute("docid", context.docId)
        doc.appendChild(layer)

        val spanList = doc.createElement("spanList")
        layer.appendChild(spanList)

        var i = 0
        context.morpho?.forEach { (spanString, mfs) ->
            i++
            val offsets = spanString.split("-")
            val spanNode = doc.createElement("span")
            spanNode.setAttribute("id", "t_$i")
            spanNode.setAttribute("from", offsets[0])
            spanNode.setAttribute("to", offsets[1])

            // Split values by | to handle multiple interpretations
            val lemmas = if (mfs.lemma != "_") mfs.lemma!!.split("|") else emptyList()
            val uposList = if (mfs.upos != "_") mfs.upos!!.split("|") else emptyList()
            val xposList = if (mfs.xpos != "_") mfs.xpos!!.split("|") else emptyList()
            val featsList = if (mfs.feats != "_") mfs.feats!!.split("|") else emptyList()
            val miscList = if (mfs.misc != "_") mfs.misc!!.split("|") else emptyList()

            val maxLen = maxOf(lemmas.size, uposList.size, xposList.size, featsList.size, miscList.size).coerceAtLeast(1)

            for (j in 0 until maxLen) {
                // fs element
                val fs = doc.createElement("fs")
                fs.setAttribute("type", "lex")
                fs.setAttribute("xmlns", "http://www.tei-c.org/ns/1.0")
                spanNode.appendChild(fs)
                val f = doc.createElement("f")
                f.setAttribute("name", "lex")
                fs.appendChild(f)

                // Inner fs element
                val innerFs = doc.createElement("fs")
                f.appendChild(innerFs)

                if (j < lemmas.size) {
                    val innerF = doc.createElement("f")
                    innerF.setAttribute("name", "lemma")
                    innerF.textContent = lemmas[j]
                    innerFs.appendChild(innerF)
                }
                if (j < uposList.size) {
                    val innerF = doc.createElement("f")
                    innerF.setAttribute("name", "upos")
                    innerF.textContent = uposList[j]
                    innerFs.appendChild(innerF)
                }
                if (j < xposList.size) {
                    val innerF = doc.createElement("f")
                    innerF.setAttribute("name", "pos")
                    innerF.textContent = xposList[j]
                    innerFs.appendChild(innerF)
                }
                if (j < featsList.size) {
                    val innerF = doc.createElement("f")
                    innerF.setAttribute("name", "msd")
                    innerF.textContent = featsList[j]
                    innerFs.appendChild(innerF)
                }
                if (j < miscList.size && miscList[j].matches(Regex("^[0-9.]+$"))) {
                    val innerF = doc.createElement("f")
                    innerF.setAttribute("name", "certainty")
                    innerF.textContent = miscList[j]
                    innerFs.appendChild(innerF)
                }
            }

            spanList.appendChild(spanNode)
        }
        
        return transformToString(doc)
    }

    /**
     * Format dependency annotations as KorAP-XML.
     */
    fun formatDependency(context: OutputContext, dBuilder: DocumentBuilder): StringBuilder {
        val doc: Document = dBuilder.newDocument()

        // Root element
        val layer = doc.createElement("layer")
        layer.setAttribute("xmlns", "http://ids-mannheim.de/ns/KorAP")
        layer.setAttribute("version", "KorAP-0.4")
        layer.setAttribute("docid", context.docId)
        doc.appendChild(layer)

        val spanList = doc.createElement("spanList")
        layer.appendChild(spanList)

        var i = 0
        var s = 0
        var n = 0
        val sortedKeys = context.morpho?.keys?.sortedBy { it.split("-")[0].toInt() }

        sortedKeys?.forEach { spanString ->
            val mfs = context.morpho?.get(spanString)
            val offsets = spanString.split("-")
            if(offsets.size != 2) {
                LOGGER.warning("Invalid span: $spanString in ${context.docId}")
                return@forEach
            }
            if (offsets[0].toInt() > context.sentences!!.elementAt(s).to) {
                s++
                n = i
            }
            i++
            if (mfs!!.deprel == "_") {
                return@forEach
            }

            val spanNode = doc.createElement("span")
            spanNode.setAttribute("id", "s${s + 1}_n${i - n}")
            spanNode.setAttribute("from", offsets[0])
            spanNode.setAttribute("to", offsets[1])

            // rel element
            val rel = doc.createElement("rel")
            rel.setAttribute("label", mfs.deprel)

            // inner span element
            val innerSpan = doc.createElement("span")
            val headInt = if(mfs.head == "_") 0 else Integer.parseInt(mfs.head) - 1
            if (headInt < 0) {
                innerSpan.setAttribute("from", context.sentences.elementAt(s).from.toString())
                innerSpan.setAttribute("to",  context.sentences.elementAt(s).to.toString())
            } else {
                if (headInt + n >= context.morpho.size) {
                    LOGGER.warning("Head index out of bounds: ${headInt+n} >= ${context.morpho.size} in ${context.docId}")
                    return@forEach
                } else {
                    val destSpanString = sortedKeys.elementAt(headInt + n)
                    val destOffsets = destSpanString.split("-")
                    innerSpan.setAttribute("from", destOffsets[0])
                    innerSpan.setAttribute("to", destOffsets[1])
                }
            }
            rel.appendChild(innerSpan)
            spanNode.appendChild(rel)
            spanList.appendChild(spanNode)
        }
        
        return transformToString(doc)
    }

    /**
     * Format constituency annotations as KorAP-XML.
     */
    fun formatConstituency(context: OutputContext, dBuilder: DocumentBuilder): StringBuilder {
        val doc: Document = dBuilder.newDocument()

        // Root element
        val layer = doc.createElement("layer")
        layer.setAttribute("xmlns", "http://ids-mannheim.de/ns/KorAP")
        layer.setAttribute("version", "KorAP-0.4")
        layer.setAttribute("docid", context.docId)
        doc.appendChild(layer)

        val spanList = doc.createElement("spanList")
        layer.appendChild(spanList)

        val trees = context.constituencyTrees
        if (trees == null || trees.isEmpty()) {
            LOGGER.warning("No constituency trees found for ${context.docId}")
            return StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
        }

        // Process each tree
        trees.forEach { tree ->
            tree.nodes.forEach { node ->
                // Create span element
                val spanNode = doc.createElement("span")
                spanNode.setAttribute("id", node.id)
                spanNode.setAttribute("from", node.from.toString())
                spanNode.setAttribute("to", node.to.toString())

                // Create fs element for the constituency label
                val fs = doc.createElement("fs")
                fs.setAttribute("xmlns", "http://www.tei-c.org/ns/1.0")
                fs.setAttribute("type", "node")

                val f = doc.createElement("f")
                f.setAttribute("name", "const")
                f.textContent = node.label
                fs.appendChild(f)

                spanNode.appendChild(fs)

                // Add rel elements for children
                node.children.forEach { child ->
                    val rel = doc.createElement("rel")
                    rel.setAttribute("label", "dominates")

                    when (child) {
                        is ConstituencyParserBridge.ConstituencyChild.NodeRef -> {
                            rel.setAttribute("target", child.targetId)
                        }
                        is ConstituencyParserBridge.ConstituencyChild.MorphoRef -> {
                            rel.setAttribute("uri", "morpho.xml#${child.morphoId}")
                        }
                    }

                    spanNode.appendChild(rel)
                }

                spanList.appendChild(spanNode)
            }
        }

        return transformToString(doc, indentAmount = "3")
    }

    /**
     * Transform DOM document to formatted XML string.
     */
    private fun transformToString(doc: Document, indentAmount: String = "1"): StringBuilder {
        val transformerFactory = TransformerFactory.newInstance()
        val transformer = transformerFactory.newTransformer()
        transformer.setOutputProperty(OutputKeys.INDENT, "yes")
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no")
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", indentAmount)
        val domSource = DOMSource(doc)
        val streamResult = StreamResult(StringWriter())
        transformer.transform(domSource, streamResult)

        return StringBuilder(streamResult.writer.toString())
    }
}
