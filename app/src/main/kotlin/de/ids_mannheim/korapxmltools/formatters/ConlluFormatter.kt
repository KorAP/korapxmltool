package de.ids_mannheim.korapxmltools.formatters

import de.ids_mannheim.korapxmltools.ConstituencyParserBridge
import de.ids_mannheim.korapxmltools.KorapXmlTool
import de.ids_mannheim.korapxmltools.NonBmpString
import java.util.logging.Logger
import kotlin.math.min

/**
 * Formatter for CoNLL-U output format.
 * Includes support for morphological annotations, dependencies, and constituency trees.
 */
object ConlluFormatter : OutputFormatter {
    override val formatName: String = "conllu"
    
    private val LOGGER: Logger = Logger.getLogger(ConlluFormatter::class.java.name)

    override fun format(context: OutputContext): StringBuilder {
        var tokenIndex = 0
        var realTokenIndex = 0
        var sentenceIndex = 0
        
        val sentencesArr = context.sentences
        val tokensArr = context.tokens
        val textVal = context.text
        
        // Build constituency comments if available
        val constituencyComments = buildConstituencyComments(
            context.docId, tokensArr, sentencesArr, textVal, context.constituencyTrees
        )
        
        // Start with document metadata
        val output = StringBuilder("# foundry = ${context.foundry}\n# filename = ${context.fileName}\n# text_id = ${context.docId}\n")
            .append(tokenOffsetsInSentence(sentencesArr, sentenceIndex, realTokenIndex, tokensArr))
        
        // Helper to append constituency comment for a sentence
        fun appendConstituencyComment(sentIdx: Int) {
            val comment = constituencyComments[sentIdx]
            if (!comment.isNullOrBlank()) {
                output.append("# constituency = ").append(comment).append("\n")
            }
        }
        
        appendConstituencyComment(sentenceIndex)
        
        // Add metadata if requested
        if (context.extractMetadataRegex.isNotEmpty()) {
            output.append(context.metadata?.joinToString("\t", prefix = "# metadata=", postfix = "\n") ?: "")
        }
        
        var previousSpanStart = 0
        if (tokensArr == null || tokensArr.isEmpty()) {
            return output
        }
        
        // Build offset-to-index mapping for resolving dependency HEADs
        val offsetToIndex = mutableMapOf<String, Int>()
        tokensArr.forEachIndexed { index, span ->
            offsetToIndex["${span.from}-${span.to}"] = index + 1 // CoNLL-U is 1-indexed
        }
        
        // Take a snapshot of the morpho map to avoid concurrent modification
        val morphoSnapshot: Map<String, KorapXmlTool.MorphoSpan> = context.morpho?.toMap() ?: emptyMap()
        
        // Helper function to resolve HEAD values (offset notation to index)
        fun resolveHeadValue(raw: String?): String {
            if (raw == null || raw == "_") return "_"
            return if (raw.contains("-")) {
                val idx = offsetToIndex[raw]
                if (idx != null) idx.toString() else "0"
            } else raw
        }
        
        // Process each token
        tokensArr.forEach { span ->
            tokenIndex++
            
            // Check if we're starting a new sentence
            if (sentencesArr != null && (sentenceIndex >= sentencesArr.size || span.from >= sentencesArr[sentenceIndex].to)) {
                output.append("\n")
                sentenceIndex++
                tokenIndex = 1
                output.append(tokenOffsetsInSentence(sentencesArr, sentenceIndex, realTokenIndex, tokensArr))
                appendConstituencyComment(sentenceIndex)
            }
            
            // Add extracted attributes if requested
            if (context.extractAttributesRegex.isNotEmpty() && context.extraFeatures != null) {
                for (i in previousSpanStart until span.from + 1) {
                    if (context.extraFeatures.containsKey("$i")) {
                        output.append(context.extraFeatures["$i"])
                        context.extraFeatures.remove("$i")
                    }
                }
                previousSpanStart = span.from + 1
            }
            
            // Get token text safely
            var tokenText: String = if (textVal != null) {
                val safeFrom = span.from.coerceIn(0, textVal.length)
                val safeTo = span.to.coerceIn(safeFrom, textVal.length)
                textVal.substring(safeFrom, safeTo)
            } else "_"
            
            if (tokenText.isBlank()) {
                LOGGER.fine("Replacing empty/blank token at offset ${span.from}-${span.to} in document ${context.docId} with underscore")
                tokenText = "_"
            }
            
            // Output the token with morphological annotations if available
            val key = "${span.from}-${span.to}"
            if (morphoSnapshot.containsKey(key)) {
                val mfs = morphoSnapshot[key]
                if (mfs != null) {
                    val miscWithOffset = if (context.includeOffsetsInMisc) {
                        val existing = mfs.misc ?: "_"
                        val isMulti = (mfs.xpos != null && mfs.xpos!!.contains("|")) || (mfs.upos != null && mfs.upos!!.contains("|"))
                        val miscVal = if (!isMulti && existing.matches(Regex("^[0-9.]+$"))) "_" else existing
                        
                        if (miscVal == "_") "Offset=${span.from}-${span.to}" else "${miscVal}|Offset=${span.from}-${span.to}"
                    } else {
                        val existing = mfs.misc ?: "_"
                        val isMulti = (mfs.xpos != null && mfs.xpos!!.contains("|")) || (mfs.upos != null && mfs.upos!!.contains("|"))
                        if (!isMulti && existing.matches(Regex("^[0-9.]+$"))) "_" else existing
                    }
                    
                    try {
                        // Sort multiple POS annotations by descending probability
                        val (sortedUpos, sortedXpos, sortedMisc) = sortByProbability(mfs.upos ?: "_", mfs.xpos ?: "_", miscWithOffset)
                        
                        // Sort multiple lemma annotations by descending probability
                        val sortedLemma = sortLemmaByProbability(mfs.lemma ?: "_", mfs.xpos ?: "_", miscWithOffset)
                        
                        output.append(
                            printConlluToken(
                                tokenIndex,
                                tokenText,
                                sortedLemma,
                                sortedUpos,
                                sortedXpos,
                                mfs.feats ?: "_",
                                resolveHeadValue(mfs.head),
                                mfs.deprel ?: "_",
                                mfs.deps ?: "_",
                                sortedMisc,
                                context.columns,
                                context.compatibilityMode,
                                context.tokenSeparator
                            )
                        )
                    } catch (e: NullPointerException) {
                        LOGGER.warning("NPE processing morpho for ${context.docId} at ${span.from}-${span.to}: ${e.message}")
                        val miscWithOffset = if (context.includeOffsetsInMisc) {
                            "Offset=${span.from}-${span.to}"
                        } else "_"
                        output.append(
                            printConlluToken(
                                tokenIndex, tokenText, misc = miscWithOffset, 
                                columns = context.columns, compatibilityMode = context.compatibilityMode,
                                tokenSeparator = context.tokenSeparator
                            )
                        )
                    }
                } else {
                    val miscWithOffset = if (context.includeOffsetsInMisc) {
                        "Offset=${span.from}-${span.to}"
                    } else "_"
                    output.append(
                        printConlluToken(
                            tokenIndex, tokenText, misc = miscWithOffset,
                            columns = context.columns, compatibilityMode = context.compatibilityMode,
                            tokenSeparator = context.tokenSeparator
                        )
                    )
                }
            } else {
                val miscWithOffset = if (context.includeOffsetsInMisc) {
                    "Offset=${span.from}-${span.to}"
                } else "_"
                output.append(
                    printConlluToken(
                        tokenIndex, tokenText, misc = miscWithOffset,
                        columns = context.columns, compatibilityMode = context.compatibilityMode,
                        tokenSeparator = context.tokenSeparator
                    )
                )
            }
            
            realTokenIndex++
        }
        
        return output
    }
    
    /**
     * Build constituency tree comments for each sentence
     */
    private fun buildConstituencyComments(
        docId: String,
        tokensArr: Array<KorapXmlTool.Span>?,
        sentencesArr: Array<KorapXmlTool.Span>?,
        textVal: NonBmpString?,
        trees: List<ConstituencyParserBridge.ConstituencyTree>?
    ): Map<Int, String> {
        if (tokensArr.isNullOrEmpty() || textVal == null || trees.isNullOrEmpty()) return emptyMap()
        
        data class TokenInfo(val from: Int, val to: Int, val surface: String)
        
        val tokenInfos = tokensArr.map { span ->
            val safeFrom = span.from.coerceIn(0, textVal.length)
            val safeTo = span.to.coerceIn(safeFrom, textVal.length)
            val surface = if (safeFrom < safeTo) {
                textVal.substring(safeFrom, safeTo)
            } else {
                "_"
            }
            TokenInfo(safeFrom, safeTo, surface.ifBlank { "_" })
        }
        
        fun tokensInRange(from: Int, to: Int): List<TokenInfo> =
            tokenInfos.filter { it.from >= from && it.to <= to }
        
        fun escapeParens(value: String): String =
            value.replace("(", "-LRB-").replace(")", "-RRB-")
        
        val comments = mutableMapOf<Int, String>()
        
        trees.forEach { tree ->
            if (tree.nodes.isEmpty()) return@forEach
            
            val nodeById = tree.nodes.associateBy { it.id }
            val referencedNodeIds = tree.nodes.flatMap { node ->
                node.children.mapNotNull { child ->
                    when (child) {
                        is ConstituencyParserBridge.ConstituencyChild.NodeRef -> child.targetId
                        is ConstituencyParserBridge.ConstituencyChild.MorphoRef -> child.morphoId.takeIf { nodeById.containsKey(it) }
                    }
                }
            }.toSet()
            val rootNode = tree.nodes.firstOrNull { it.id !in referencedNodeIds } ?: tree.nodes.first()
            val visited = mutableSetOf<String>()
            val sentenceIdx = sentencesArr
                ?.indexOfFirst { rootNode.from >= it.from && rootNode.to <= it.to }
                ?.takeIf { it >= 0 }
                ?: 0
            val sentenceTokens = sentencesArr
                ?.getOrNull(sentenceIdx)
                ?.let { sentSpan -> tokensInRange(sentSpan.from, sentSpan.to) }
                ?: tokenInfos
            var sentenceTokenCursor = 0
            
            fun render(node: ConstituencyParserBridge.ConstituencyNode): String? {
                if (!visited.add(node.id)) return null
                
                val childStrings = node.children.mapNotNull { child ->
                    when (child) {
                        is ConstituencyParserBridge.ConstituencyChild.NodeRef -> {
                            val childNode = nodeById[child.targetId] ?: return@mapNotNull null
                            render(childNode)
                        }
                        is ConstituencyParserBridge.ConstituencyChild.MorphoRef -> {
                            val nextToken = sentenceTokens.getOrNull(sentenceTokenCursor++)
                                ?: return@mapNotNull null
                            val tokenText = escapeParens(nextToken.surface)
                            val label = escapeParens(nodeById[child.morphoId]?.label ?: "TOK")
                            if (label == "TOK") tokenText else "($label $tokenText)"
                        }
                    }
                }.filter { it.isNotBlank() }
                
                val label = escapeParens(node.label.ifBlank { "ROOT" })
                if (childStrings.isEmpty()) {
                    val fallbackTokens = tokensInRange(node.from, node.to)
                    return if (fallbackTokens.isNotEmpty()) {
                        "($label ${fallbackTokens.joinToString(" ") { escapeParens(it.surface) }})"
                    } else {
                        "($label)"
                    }
                }
                
                return "($label ${childStrings.joinToString(" ")})"
            }
            
            val rendered = render(rootNode) ?: return@forEach
            
            comments.merge(sentenceIdx, rendered) { old, new -> "$old | $new" }
        }
        
        return comments
    }
    
    /**
     * Print a single CoNLL-U token with the specified number of columns
     */
    private fun printConlluToken(
        tokenIndex: Int,
        token: String,
        lemma: String = "_",
        upos: String = "_",
        xpos: String = "_",
        feats: String = "_",
        head: String = "_",
        deprel: String = "_",
        deps: String = "_",
        misc: String = "_",
        columns: Int = 10,
        compatibilityMode: Boolean = false,
        tokenSeparator: String = "\n"
    ): String {
        val myUpos = if (compatibilityMode && upos == "_") xpos else upos
        return when (columns) {
            1 -> ("$token\n")
            10 -> ("$tokenIndex\t$token\t$lemma\t$myUpos\t$xpos\t$feats\t$head\t$deprel\t$deps\t$misc$tokenSeparator")
            else -> {
                val fields = listOf(
                    tokenIndex.toString(), token, lemma, myUpos, xpos, feats, head, deprel, deps, misc
                )
                fields.subList(0, min(columns, 10)).joinToString("\t", postfix = tokenSeparator)
            }
        }
    }
    
    /**
     * Sort multiple POS annotations by descending probability.
     * If probabilities are found in misc field, reorder upos, xpos, and misc accordingly.
     */
    private fun sortByProbability(upos: String, xpos: String, misc: String): Triple<String, String, String> {
        // Extract probabilities from misc field (exclude Offset= part if present)
        val miscParts = misc.split("|")
        val probabilities = mutableListOf<Double?>()
        val nonProbParts = mutableListOf<String>()
        
        for (part in miscParts) {
            if (part.startsWith("Offset=")) {
                nonProbParts.add(part)
            } else {
                val prob = part.toDoubleOrNull()
                if (prob != null) {
                    probabilities.add(prob)
                } else {
                    nonProbParts.add(part)
                }
            }
        }
        
        // If we don't have probabilities or they don't match POS count, return as-is
        val uposParts = if (upos == "_") emptyList() else upos.split("|")
        val xposParts = if (xpos == "_") emptyList() else xpos.split("|")
        
        // Use xpos as the primary reference for multiple annotations
        if (probabilities.isEmpty() || probabilities.size != xposParts.size) {
            return Triple(upos, xpos, misc)
        }
        
        // Create indexed list for sorting
        val indexed = xposParts.mapIndexed { index, tag ->
            val prob = probabilities.getOrNull(index) ?: 0.0
            val uposTag = uposParts.getOrNull(index) ?: "_"
            Triple(prob, uposTag, tag)
        }
        
        // Sort by descending probability
        val sorted = indexed.sortedByDescending { it.first }
        
        // Reconstruct the fields
        val sortedUpos = if (uposParts.isNotEmpty()) {
            sorted.map { it.second }.joinToString("|")
        } else "_"
        
        val sortedXpos = sorted.map { it.third }.joinToString("|")
        
        val sortedProbs = sorted.map { "%.3f".format(it.first) }.joinToString("|")
        val sortedMisc = if (nonProbParts.isNotEmpty()) {
            (listOf(sortedProbs) + nonProbParts).joinToString("|")
        } else {
            sortedProbs
        }
        
        return Triple(sortedUpos, sortedXpos, sortedMisc)
    }
    
    /**
     * Sort multiple lemma annotations by descending probability.
     * Uses the same probability ordering as POS annotations from the misc field.
     */
    private fun sortLemmaByProbability(lemma: String, xpos: String, misc: String): String {
        if (lemma == "_") return "_"
        
        // Extract probabilities from misc field (exclude Offset= part if present)
        val miscParts = misc.split("|")
        val probabilities = mutableListOf<Double>()
        
        for (part in miscParts) {
            if (!part.startsWith("Offset=")) {
                val prob = part.toDoubleOrNull()
                if (prob != null) {
                    probabilities.add(prob)
                }
            }
        }
        
        val lemmaParts = lemma.split("|").distinct()
        val xposParts = if (xpos == "_") emptyList() else xpos.split("|")
        
        // If we don't have probabilities or they don't match the count, return distinct lemmas as-is
        if (probabilities.isEmpty() || probabilities.size != xposParts.size || lemmaParts.size != xposParts.size) {
            return lemmaParts.joinToString("|")
        }
        
        // Create indexed list for sorting
        val indexed = lemmaParts.mapIndexed { index, lemmaValue ->
            val prob = probabilities.getOrNull(index) ?: 0.0
            Pair(prob, lemmaValue)
        }
        
        // Sort by descending probability and return
        return indexed.sortedByDescending { it.first }.map { it.second }.joinToString("|")
    }
    
    /**
     * Generate comment lines with token offset information for a sentence
     */
    private fun tokenOffsetsInSentence(
        sentences: Array<KorapXmlTool.Span>?,
        sentenceIndex: Int,
        tokenIndex: Int,
        tokens: Array<KorapXmlTool.Span>?
    ): String {
        if (sentences == null || sentenceIndex !in sentences.indices) return ""
        val toks = tokens ?: return ""
        if (toks.isEmpty() || tokenIndex !in toks.indices) return ""
        
        val sentenceEndOffset = sentences[sentenceIndex].to
        var i = tokenIndex
        val startOffsetsString = StringBuilder()
        val endOffsetsString = StringBuilder()
        
        while (i < toks.size && toks[i].to <= sentenceEndOffset) {
            startOffsetsString.append(" ").append(toks[i].from)
            endOffsetsString.append(" ").append(toks[i].to)
            i++
        }
        
        return StringBuilder()
            .append("# start_offsets = ").append(toks[tokenIndex].from).append(startOffsetsString).append("\n")
            .append("# end_offsets = ").append(sentenceEndOffset).append(endOffsetsString).append("\n")
            .toString()
    }
}
