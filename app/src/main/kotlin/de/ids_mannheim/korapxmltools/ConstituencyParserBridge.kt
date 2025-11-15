package de.ids_mannheim.korapxmltools

abstract class ConstituencyParserBridge : AnnotationToolBridge {

    data class ConstituencyTree(
        val sentenceId: String,
        val nodes: List<ConstituencyNode>
    )

    data class ConstituencyNode(
        val id: String,
        val label: String,
        val from: Int,
        val to: Int,
        val children: List<ConstituencyChild>
    )

    sealed class ConstituencyChild {
        data class NodeRef(val targetId: String) : ConstituencyChild()
        data class MorphoRef(val morphoId: String) : ConstituencyChild()
    }

    /**
     * Parse text and return constituency trees for each sentence.
     *
     * @param tokens Array of token spans
     * @param morpho Map of morphological annotations (may be null or incomplete)
     * @param sentenceSpans Array of sentence spans
     * @param text The full text as NonBmpString
     * @return List of constituency trees, one per sentence
     */
    abstract fun parseConstituency(
        tokens: Array<KorapXmlTool.Span>,
        morpho: MutableMap<String, KorapXmlTool.MorphoSpan>?,
        sentenceSpans: Array<KorapXmlTool.Span>?,
        text: NonBmpString
    ): List<ConstituencyTree>

    /**
     * Optionally update morpho map with POS tags from constituency parser.
     * Default implementation does nothing.
     */
    open fun updateMorphoFromConstituency(
        tokens: Array<KorapXmlTool.Span>,
        morpho: MutableMap<String, KorapXmlTool.MorphoSpan>?,
        trees: List<ConstituencyTree>,
        text: NonBmpString
    ): MutableMap<String, KorapXmlTool.MorphoSpan>? {
        return morpho
    }

    // Implementation required by AnnotationToolBridge but not used for constituency parsing
    override fun tagSentence(
        sentenceTokens: MutableList<String>,
        sentenceTokenOffsets: MutableList<String>,
        morphoMap: MutableMap<String, KorapXmlTool.MorphoSpan>?
    ) {
        throw UnsupportedOperationException("Constituency parsers use parseConstituency() instead")
    }
}
