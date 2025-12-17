package de.ids_mannheim.korapxmltools.formatters

import de.ids_mannheim.korapxmltools.KorapXmlTool.MorphoSpan
import de.ids_mannheim.korapxmltools.KorapXmlTool.Span
import de.ids_mannheim.korapxmltools.NonBmpString
import java.util.logging.Logger

/**
 * Generator for Krill JSON output format.
 * Krill is a search engine format used by KorAP that includes metadata, tokens, 
 * morphological annotations, dependencies, and structural spans.
 */
object KrillJsonGenerator {
    private val LOGGER = Logger.getLogger(KrillJsonGenerator::class.java.name)
    private val BASE_STRUCTURE_FOUNDRIES = setOf("base", "dereko")

    /**
     * Data class representing a complete Krill text with all annotations.
     */
    data class KrillTextData(
        var textId: String,
        var textContent: NonBmpString? = null,
        var headerMetadata: MutableMap<String, Any> = mutableMapOf(),
        var tokens: Array<Span>? = null,
        var sentences: Array<Span>? = null,
        var morphoByFoundry: MutableMap<String, MutableMap<String, MorphoSpan>> = mutableMapOf(),
        var structureSpans: MutableList<StructureSpan> = mutableListOf(),
        var extractedAttributes: MutableMap<String, String> = mutableMapOf(),
        var lpSentencesCollected: Boolean = false,
        var sentencesCollectedByFoundry: MutableSet<String> = mutableSetOf(),
        var constituencyCollectedByFoundry: MutableSet<String> = mutableSetOf()
    )

    /**
     * Represents a structural span (sentence, paragraph, text, etc.) in Krill format.
     */
    data class StructureSpan(
        val layer: String,  // e.g., "base/s:s", "dereko/s:p"
        val from: Int,
        val to: Int,
        val tokenFrom: Int,
        val tokenTo: Int,
        val depth: Int,
        val attributes: Map<String, String> = emptyMap()
    )

    /**
     * Generate complete Krill JSON for a text.
     * 
     * @param textData The complete text data with annotations
     * @param corpusMetadata Map of corpus-level metadata
     * @param docMetadata Map of document-level metadata
     * @param includeNonWordTokens Whether to include non-word tokens in output
     * @return JSON string in Krill format
     */
    fun generate(
        textData: KrillTextData,
        corpusMetadata: Map<String, MutableMap<String, Any>>,
        docMetadata: Map<String, MutableMap<String, Any>>,
        includeNonWordTokens: Boolean
    ): String {
        val sb = StringBuilder()
        sb.append("{")

        // @context, @type, and version
        sb.append("\"@context\":\"http://korap.ids-mannheim.de/ns/koral/0.4/context.jsonld\",")
        sb.append("\"@type\":\"koral:corpus\",")
        sb.append("\"version\":\"0.4\",")

        // fields (metadata)
        sb.append("\"fields\":[")
        val fields = mutableListOf<String>()

        // Extract corpus, doc, and text sigle from textId (e.g., "WUD24_I0083.95367")
        // Convert underscores to slashes for proper format
        val textIdWithSlashes = textData.textId.replace("_", "/").replace(".", "/")
        val sigleParts = textIdWithSlashes.split("/")
        if (sigleParts.size >= 3) {
            fields.add(jsonObject(listOf(
                "value" to jsonString(sigleParts[0]),
                "type" to jsonString("type:string"),
                "@type" to jsonString("koral:field"),
                "key" to jsonString("corpusSigle")
            )))
            fields.add(jsonObject(listOf(
                "@type" to jsonString("koral:field"),
                "value" to jsonString("${sigleParts[0]}/${sigleParts[1]}"),
                "type" to jsonString("type:string"),
                "key" to jsonString("docSigle")
            )))
            fields.add(jsonObject(listOf(
                "@type" to jsonString("koral:field"),
                "type" to jsonString("type:string"),
                "value" to jsonString(textIdWithSlashes),
                "key" to jsonString("textSigle")
            )))
        }

        // Merge corpus and doc metadata into text metadata (corpus < doc < text precedence)
        val corpusSigle = textIdWithSlashes.split("/")[0]
        val docSigle = textIdWithSlashes.split("/").take(2).joinToString("/")

        // First apply corpus-level metadata (lowest priority) - only if not already set with non-empty value
        corpusMetadata[corpusSigle]?.forEach { (key, value) ->
            val currentValue = textData.headerMetadata[key]
            val shouldInherit = when (currentValue) {
                null -> true
                is String -> currentValue.isBlank()
                else -> false
            }
            if (shouldInherit && value != null) {
                when (value) {
                    is String -> if (value.isNotBlank()) textData.headerMetadata[key] = value
                    is List<*> -> if (value.isNotEmpty()) textData.headerMetadata[key] = value
                    is Map<*, *> -> if (value.isNotEmpty()) textData.headerMetadata[key] = value
                    else -> textData.headerMetadata[key] = value
                }
            }
        }

        // Then apply doc-level metadata (medium priority) - only if not already set with non-empty value
        docMetadata[docSigle]?.forEach { (key, value) ->
            val currentValue = textData.headerMetadata[key]
            val shouldInherit = when (currentValue) {
                null -> true
                is String -> currentValue.isBlank()
                else -> false
            }
            if (shouldInherit && value != null) {
                when (value) {
                    is String -> if (value.isNotBlank()) textData.headerMetadata[key] = value
                    is List<*> -> if (value.isNotEmpty()) textData.headerMetadata[key] = value
                    is Map<*, *> -> if (value.isNotEmpty()) textData.headerMetadata[key] = value
                    else -> textData.headerMetadata[key] = value
                }
            }
        }

        // Text-level metadata is already in textData.headerMetadata (highest priority)

        // Add additional metadata fields from header with correct types
        val fieldOrder = listOf(
            "corpusEditor", "distributor", "editor", "translator", "externalLink", "publisher", "reference",
            "creationDate", "pubDate", "textClass", "award", "availability", "language",
            "ISBN", "URN", "pubPlace", "pubPlaceKey",
            "textType", "textTypeArt", "textTypeRef", "textDomain", "textColumn",
            "author", "title", "subTitle", "corpusTitle", "corpusSubTitle", "docTitle", "docAuthor",
            "textExternalLinks", "tokenSource"
        )

        fieldOrder.forEach { key ->
            val value = textData.headerMetadata[key] ?: return@forEach

            // Determine field type and value format
            val (fieldType, fieldValue) = when (key) {
                "creationDate", "pubDate" -> {
                    "type:date" to jsonString(value.toString())
                }
                "textClass", "award" -> {
                    "type:keywords" to when (value) {
                        is List<*> -> jsonArray(value.map { jsonString(it.toString()) })
                        else -> jsonArray(listOf(jsonString(value.toString())))
                    }
                }
                "availability", "language", "ISBN", "pubPlace", "pubPlaceKey",
                "textType", "textTypeArt", "textTypeRef", "textDomain", "textColumn" -> {
                    "type:string" to jsonString(value.toString())
                }
                "URN" -> {
                    // URN is stored as a map with urn and url keys
                    when (value) {
                        is Map<*, *> -> {
                            val urn = value["urn"]?.toString() ?: ""
                            val url = value["url"]?.toString() ?: ""
                            val encodedUrn = urn.urlEncode()
                            val encodedUrl = url.urlEncode()
                            "type:attachement" to jsonString("data:application/x.korap-link;title=$encodedUrn,$encodedUrl")
                        }
                        else -> {
                            // Fallback if URN is stored as plain string (shouldn't happen)
                            "type:string" to jsonString(value.toString())
                        }
                    }
                }
                "translator" -> {
                    // Check environment variable for type
                    val translatorAsText = System.getenv("K2K_TRANSLATOR_TEXT")?.isNotEmpty() == true ||
                        System.getProperty("K2K_TRANSLATOR_TEXT")?.isNotEmpty() == true
                    if (translatorAsText) {
                        "type:text" to jsonString(value.toString())
                    } else {
                        "type:attachement" to jsonString("data:,${value.toString()}")
                    }
                }
                "publisher" -> {
                    // Check environment variable for type
                    val publisherAsString = System.getenv("K2K_PUBLISHER_STRING")?.isNotEmpty() == true ||
                        System.getProperty("K2K_PUBLISHER_STRING")?.isNotEmpty() == true
                    if (publisherAsString) {
                        "type:string" to jsonString(value.toString())
                    } else {
                        "type:attachement" to jsonString("data:,${value.toString()}")
                    }
                }
                "author", "title", "subTitle", "corpusTitle", "corpusSubTitle", "docTitle", "docAuthor" -> {
                    "type:text" to jsonString(value.toString())
                }
                "externalLink" -> {
                    val url = value.toString()
                    // Extract title from corpus/publisher metadata if available
                    val title = textData.headerMetadata["publisher"]?.toString() ?: "Link"
                    val encodedUrl = url.replace(":", "%3A").replace("/", "%2F")
                    "type:attachement" to jsonString("data:application/x.korap-link;title=$title,$encodedUrl")
                }
                "textExternalLinks" -> {
                    val url = value.toString()
                    val title = textData.headerMetadata["publisher"]?.toString() ?: "Link"
                    val encodedUrl = url.replace(":", "%3A").replace("/", "%2F")
                    "type:attachement" to jsonString("data:application/x.korap-link;title=$title,$encodedUrl")
                }
                "tokenSource" -> {
                    // tokenSource is a string foundry reference like "base#tokens"
                    "type:string" to jsonString(value.toString())
                }
                else -> {
                    // corpusEditor, distributor, editor, reference - all ATTACHMENT
                    "type:attachement" to jsonString("data:,${value.toString()}")
                }
            }

            fields.add(jsonObject(listOf(
                "key" to jsonString(key),
                "@type" to jsonString("koral:field"),
                "value" to fieldValue,
                "type" to jsonString(fieldType)
            )))
        }

        sb.append(fields.joinToString(","))
        sb.append("],")

        // data section
        sb.append("\"data\":{")
        sb.append("\"text\":${jsonString(textData.textContent?.toString() ?: "")},")

        val sentenceSpanFoundries = textData.structureSpans.foundriesWithSentenceSpans()
        val constituencySpanFoundries = textData.structureSpans.foundriesWithConstituencySpans()
        val externalSentenceFoundries = sentenceSpanFoundries.filterNot { it in BASE_STRUCTURE_FOUNDRIES }.toSortedSet()
        val externalConstitFoundries = constituencySpanFoundries.filterNot { it in BASE_STRUCTURE_FOUNDRIES }.toSortedSet()
        val layerInfos = mutableListOf<String>()
        if (textData.sentences != null) {
            layerInfos.add("dereko/s=spans")
        }
        externalSentenceFoundries.forEach { layerInfos.add("$it/s=spans") }
        externalConstitFoundries.forEach { layerInfos.add("$it/c=spans") }

        // Collect layers by foundry type (checking what data actually exists)
        val foundryLayers = mutableMapOf<String, MutableSet<String>>()
        textData.morphoByFoundry.keys.sorted().forEach { foundry ->
            val shortFoundry = when(foundry) {
                "base" -> null
                "tree_tagger" -> "tt"
                "marmot-malt" -> "marmot"
                else -> foundry
            }
            if (shortFoundry != null) {
                val layers = foundryLayers.getOrPut(shortFoundry) { mutableSetOf() }
                val morphoData = textData.morphoByFoundry[foundry]?.values

                // Check if this foundry has dependency annotations
                val hasDependencies = morphoData?.any {
                    it.head != null && it.head != "_" && it.deprel != null && it.deprel != "_"
                } ?: false

                if (hasDependencies) {
                    layers.add("d=rels")
                }

                // Check if this foundry has lemma annotations
                val hasLemma = morphoData?.any {
                    it.lemma != null && it.lemma != "_"
                } ?: false
                if (hasLemma) {
                    layers.add("l=tokens")
                }

                // Check if this foundry has POS annotations (xpos or upos)
                val hasPos = morphoData?.any {
                    (it.xpos != null && it.xpos != "_") || (it.upos != null && it.upos != "_")
                } ?: false
                if (hasPos) {
                    layers.add("p=tokens")
                }

                // Check if this foundry has morphological features
                val hasFeatures = morphoData?.any {
                    it.feats != null && it.feats != "_"
                } ?: false
                if (hasFeatures) {
                    layers.add("m=tokens")
                }

                // Check if this foundry has UPOS (skip for tree_tagger)
                if (foundry != "tree_tagger") {
                    val hasUpos = morphoData?.any {
                        it.upos != null && it.upos != "_"
                    } ?: false
                    if (hasUpos) {
                        layers.add("u=tokens")
                    }
                }
            }
        }

        // Add foundry layers in sorted order
        foundryLayers.keys.sorted().forEach { foundry ->
            foundryLayers[foundry]?.sorted()?.forEach { layer ->
                layerInfos.add("$foundry/$layer")
            }
        }
        sb.append("\"layerInfos\":${jsonString(layerInfos.joinToString(" "))},")

        // foundries - list all foundries with their layers
        val foundries = mutableListOf<String>()

        // Add dereko if we have structure
        if (textData.sentences != null) {
            foundries.add("dereko")
            foundries.add("dereko/structure")
            foundries.add("dereko/structure/base-sentences-paragraphs-pagebreaks")
        }

        val advertisedStructureFoundries = (externalSentenceFoundries + externalConstitFoundries).toSortedSet()
        advertisedStructureFoundries.forEach { foundry ->
            if (!foundries.contains(foundry)) {
                foundries.add(foundry)
            }
            if (externalSentenceFoundries.contains(foundry)) {
                val sentencesEntry = "$foundry/sentences"
                if (!foundries.contains(sentencesEntry)) {
                    foundries.add(sentencesEntry)
                }
            }
            if (externalConstitFoundries.contains(foundry)) {
                val structureEntry = "$foundry/structure"
                if (!foundries.contains(structureEntry)) {
                    foundries.add(structureEntry)
                }
            }
        }

        // Add annotation foundries with their layers
        foundryLayers.keys.sorted().forEach { foundry ->
            // Use full name "treetagger" instead of "tt" in foundries list
            val foundryFullName = if (foundry == "tt") "treetagger" else foundry
            foundries.add(foundryFullName)
            foundryLayers[foundry]?.sorted()?.forEach { layer ->
                // Convert layer format: "d=rels" -> "dependency", "p=tokens" -> "morpho", etc.
                val layerName = when {
                    layer.startsWith("d=") -> "dependency"
                    layer.startsWith("l=") || layer.startsWith("p=") || layer.startsWith("m=") || layer.startsWith("u=") -> "morpho"
                    else -> layer.split("=")[0]
                }
                val foundryLayer = "$foundryFullName/$layerName"
                if (!foundries.contains(foundryLayer)) {
                    foundries.add(foundryLayer)
                }
            }
        }
        sb.append("\"foundries\":${jsonString(foundries.joinToString(" "))},")

        // name - field name for the data (always "tokens")
        sb.append("\"name\":\"tokens\",")

        // stream - token-level annotations
        sb.append("\"stream\":[")
        if (textData.tokens != null) {
            val streamItems = generateStream(textData, includeNonWordTokens)
            sb.append(streamItems.joinToString(","))
        }
        sb.append("]")

        sb.append("}")  // close data
        sb.append("}")  // close root

        return sb.toString()
    }

    private fun generateStream(textData: KrillTextData, includeNonWordTokens: Boolean): List<String> {
        val rawTokens = textData.tokens ?: return emptyList()
        val text = textData.textContent ?: NonBmpString("")
        val sentences = textData.sentences ?: emptyArray()
        val tokens: List<Span> = if (includeNonWordTokens || text.length == 0) {
            rawTokens.toList()
        } else {
            rawTokens.filter { span -> shouldKeepTokenForKrill(text, span) }
        }
        if (tokens.isEmpty()) {
            LOGGER.fine("No tokens remained for ${textData.textId} after filtering non-word tokens")
            return emptyList()
        }
        val result = mutableListOf<String>()

        // Build offset-to-index map for resolving dependency heads and structural spans
        val offsetToIndex = mutableMapOf<String, Int>()
        tokens.forEachIndexed { index, token ->
            offsetToIndex["${token.from}-${token.to}"] = index
        }

        // Collect inverse dependency relations and ROOT dependencies
        data class InverseDep(val dependentIndex: Int, val foundry: String, val deprel: String)
        data class RootDep(val tokenIndex: Int, val foundry: String)
        val inverseDeps = mutableMapOf<Int, MutableList<InverseDep>>()
        val rootTokens = mutableListOf<RootDep>()

        tokens.forEachIndexed { index, token ->
            val spanKey = "${token.from}-${token.to}"
            textData.morphoByFoundry.keys.forEach { foundry ->
                val morphoSpan = textData.morphoByFoundry[foundry]?.get(spanKey)
                if (morphoSpan != null && morphoSpan.head != null && morphoSpan.head != "_" && morphoSpan.deprel != null && morphoSpan.deprel != "_") {
                    val headStr = morphoSpan.head!!
                    val prefix = when(foundry) {
                        "tree_tagger" -> "tt"
                        "marmot-malt" -> "marmot"
                        else -> foundry
                    }

                    // Check if this is a ROOT dependency (head == 0)
                    if (headStr == "0" || (headStr.contains("-") && headStr.startsWith("0-"))) {
                        rootTokens.add(RootDep(index, prefix))
                    } else {
                        val resolvedHeadIndex = if (headStr.contains("-")) {
                            offsetToIndex[headStr]
                        } else {
                            val idx = headStr.toIntOrNull()
                            if (idx != null && idx > 0) idx - 1 else null
                        }

                        if (resolvedHeadIndex != null) {
                            inverseDeps.getOrPut(resolvedHeadIndex) { mutableListOf() }
                                .add(InverseDep(index, prefix, morphoSpan.deprel!!))
                        }
                    }
                }
            }
        }

        // Add base structure spans (sentences, paragraphs, text)
        val baseStructureSpans = mutableListOf<StructureSpan>()

        // Determine the actual end of text from raw tokens (before filtering)
        // This ensures base spans cover the full text including punctuation
        val textEnd = if (rawTokens.isNotEmpty()) rawTokens.last().to else 0

        // Add text span covering entire document (from start of text to end, tokenTo is exclusive)
        if (tokens.isNotEmpty()) {
            baseStructureSpans.add(StructureSpan(
                layer = "base/s:t",
                from = 0,  // Start at beginning of text
                to = textEnd,  // Use raw tokens to include all punctuation
                tokenFrom = 0,
                tokenTo = tokens.size,  // Exclusive end: one past last token index
                depth = 0,
                attributes = emptyMap()
            ))
        }

        // Create base/s:p spans that mirror dereko/s:p elements
        // For each dereko/s:p, create a corresponding base/s:p at depth 1
        textData.structureSpans.filter { it.layer.endsWith(":p") }.forEach { derekoP ->
            baseStructureSpans.add(StructureSpan(
                layer = "base/s:p",
                from = derekoP.from,
                to = derekoP.to,
                tokenFrom = -1,  // Will be resolved later
                tokenTo = -1,
                depth = 1,
                attributes = emptyMap()
            ))
        }


        // Build token-to-sentence map for ROOT edge generation
        data class SentenceInfo(val from: Int, val to: Int, val tokenFrom: Int, val tokenTo: Int)
        val tokenToSentence = mutableMapOf<Int, SentenceInfo>()

        // Add sentence spans (tokenTo is exclusive: first token after the span)
        sentences.forEachIndexed { sentIdx, sentence ->
            val sentTokens = tokens.filter { it.from >= sentence.from && it.to <= sentence.to }
            if (sentTokens.isNotEmpty()) {
                val firstTokenIdx = tokens.indexOf(sentTokens.first())
                val lastTokenIdx = tokens.indexOf(sentTokens.last())
                val sentInfo = SentenceInfo(
                    from = sentTokens.first().from,
                    to = sentTokens.last().to,
                    tokenFrom = firstTokenIdx,
                    tokenTo = lastTokenIdx + 1  // Exclusive end
                )

                // Map all tokens in this sentence to the sentence info
                for (i in firstTokenIdx until sentInfo.tokenTo) {
                    tokenToSentence[i] = sentInfo
                }

                baseStructureSpans.add(StructureSpan(
                    layer = "base/s:s",
                    from = sentInfo.from,
                    to = sentInfo.to,
                    tokenFrom = sentInfo.tokenFrom,
                    tokenTo = sentInfo.tokenTo,
                    depth = 2,
                    attributes = emptyMap()
                ))
            }
        }

        // Combine base structure spans with dereko spans
        val allStructureSpans = baseStructureSpans + textData.structureSpans

        // Resolve tokenFrom and tokenTo for structural spans
        // Note: tokenTo is exclusive (one past the last token index)
        val resolvedStructureSpans = allStructureSpans.map { span ->
            if (span.tokenFrom >= 0 && span.tokenTo >= 0) {
                // Already resolved
                span
            } else {
                // Find first and last token covered by this span
                var tokenFrom = tokens.indexOfFirst { it.from >= span.from && it.from < span.to }
                var lastTokenIndex = tokens.indexOfLast { it.to > span.from && it.to <= span.to }

                // Handle edge cases
                if (tokenFrom == -1) tokenFrom = 0
                if (lastTokenIndex == -1) lastTokenIndex = tokens.size - 1

                // tokenTo is exclusive: one past the last token
                val tokenTo = lastTokenIndex + 1

                span.copy(tokenFrom = tokenFrom, tokenTo = tokenTo)
            }
        }

        val resolvedSentenceFoundries = resolvedStructureSpans.foundriesWithSentenceSpans()
        val resolvedConstitFoundries = resolvedStructureSpans.foundriesWithConstituencySpans()
        val externalSentenceFoundries = resolvedSentenceFoundries.filterNot { it in BASE_STRUCTURE_FOUNDRIES }.toSortedSet()
        val externalConstitFoundries = resolvedConstitFoundries.filterNot { it in BASE_STRUCTURE_FOUNDRIES }.toSortedSet()

        // Group structural spans by their starting token
        val spansByToken = mutableMapOf<Int, MutableList<StructureSpan>>()
        resolvedStructureSpans.forEach { span ->
            spansByToken.getOrPut(span.tokenFrom) { mutableListOf() }.add(span)
        }

        // Count paragraph spans (name="p") from original document structure only
        // Don't count the base/s:p wrapper we added programmatically
        val paragraphCount = textData.structureSpans.count { it.layer.endsWith(":p") }
        val sentenceCountsByFoundry = resolvedStructureSpans.sentenceCountsByFoundry()
        val externalSentenceCounts = sentenceCountsByFoundry.entries
            .filter { (foundry, _) -> foundry !in BASE_STRUCTURE_FOUNDRIES }
            .sortedBy { it.key }

        tokens.forEachIndexed { index, token ->
            val tokenAnnotations = mutableListOf<String>()
            val spanKey = "${token.from}-${token.to}"

            // Add counts and structural spans only for first token
            if (index == 0) {
                if (paragraphCount > 0) {
                    tokenAnnotations.add(jsonString("-:base/paragraphs\$<i>$paragraphCount"))
                }
                if (sentences.isNotEmpty()) {
                    tokenAnnotations.add(jsonString("-:base/sentences\$<i>${sentences.size}"))
                }
                externalSentenceCounts.forEach { (foundry, count) ->
                    tokenAnnotations.add(jsonString("-:$foundry/sentences\$<i>$count"))
                }
                tokenAnnotations.add(jsonString("-:tokens\$<i>${tokens.size}"))

                // Add all structural spans that start at token 0 or cover the whole document
                val spansAtZero = spansByToken[0] ?: emptyList()
                spansAtZero.sortedWith(compareBy({ -it.depth }, { it.layer })).forEach { span ->
                    val spanAnnotation = if (span.attributes.isEmpty()) {
                        "<>:${span.layer}\$<b>64<i>${span.from}<i>${span.to}<i>${span.tokenTo}<b>${span.depth}"
                    } else {
                        // Spans with attributes get a unique ID
                        val attrId = span.depth
                        "<>:${span.layer}\$<b>64<i>${span.from}<i>${span.to}<i>${span.tokenTo}<b>${span.depth}<s>$attrId"
                    }
                    tokenAnnotations.add(jsonString(spanAnnotation))

                    // Add attribute annotations
                    span.attributes.forEach { (key, value) ->
                        val attrAnnotation = if (value.isEmpty()) {
                            "@:dereko/s:$key\$<b>17<s>${span.depth}<i>${span.tokenTo}"
                        } else {
                            "@:dereko/s:$key:${value.escapeKrillAttribute()}\$<b>17<s>${span.depth}<i>${span.tokenTo}"
                        }
                        tokenAnnotations.add(jsonString(attrAnnotation))
                    }
                }
            } else {
                // Add structural spans that start at this token
                spansByToken[index]?.sortedWith(compareBy({ -it.depth }, { it.layer }))?.forEach { span ->
                    val spanAnnotation = if (span.attributes.isEmpty()) {
                        "<>:${span.layer}\$<b>64<i>${span.from}<i>${span.to}<i>${span.tokenTo}<b>${span.depth}"
                    } else {
                        "<>:${span.layer}\$<b>64<i>${span.from}<i>${span.to}<i>${span.tokenTo}<b>${span.depth}<s>${span.depth}"
                    }
                    tokenAnnotations.add(jsonString(spanAnnotation))

                    span.attributes.forEach { (key, value) ->
                        val attrAnnotation = if (value.isEmpty()) {
                            "@:dereko/s:$key\$<b>17<s>${span.depth}<i>${span.tokenTo}"
                        } else {
                            "@:dereko/s:$key:${value.escapeKrillAttribute()}\$<b>17<s>${span.depth}<i>${span.tokenTo}"
                        }
                        tokenAnnotations.add(jsonString(attrAnnotation))
                    }
                }
            }

            // Token offset annotation
            tokenAnnotations.add(jsonString("_$index\$<i>${token.from}<i>${token.to}"))

            // Get surface form (used for both i: and s: annotations)
            // Get surface form (used for both i: and s: annotations)
            val surfaceForm = if (token.to <= text.length) {
                text.subSequence(token.from, token.to).toString()
            } else {
                ""
            }

            // Add i: annotation (lowercase surface form)
            if (surfaceForm.isNotEmpty()) {
                tokenAnnotations.add(jsonString("i:${surfaceForm.lowercase().escapeKrillValue()}"))
            }

            // Add inverse dependency annotations (<:) for dependents pointing to this token as head
            inverseDeps[index]?.sortedBy { "${it.foundry}/${it.deprel}" }?.forEach { inv ->
                tokenAnnotations.add(jsonString("<:${inv.foundry}/d:${inv.deprel.escapeKrillValue()}\$<b>32<i>${inv.dependentIndex}"))
            }

            // Collect annotations from all foundries for this token
            val sortedFoundries = textData.morphoByFoundry.keys.sorted()
            sortedFoundries.forEach { foundry ->
                val morphoSpan = textData.morphoByFoundry[foundry]?.get(spanKey)
                if (morphoSpan != null) {
                    val prefix = when(foundry) {
                        "tree_tagger" -> "tt"
                        "marmot-malt" -> "marmot"
                        "base" -> null  // Skip base for most annotations
                        else -> foundry
                    }

                    if (prefix != null) {
                        // Morphological features (sorted)
                        if (morphoSpan.feats != null && morphoSpan.feats != "_") {
                            val features = mutableListOf<String>()
                            morphoSpan.feats!!.split("|").forEach { feat ->
                                val parts = feat.split("=")
                                if (parts.size == 2) {
                                    val key = parts[0].lowercase().escapeKrillValue()
                                    val value = parts[1].lowercase().escapeKrillValue()
                                    features.add("$prefix/m:$key:$value")
                                }
                            }
                            features.sorted().forEach { tokenAnnotations.add(jsonString(it)) }
                        }

                        // POS (xpos) with optional byte encoding - sorted by descending probability
                        if (morphoSpan.xpos != null && morphoSpan.xpos != "_") {
                            val xposList = morphoSpan.xpos!!.split("|")
                            val miscList = if (morphoSpan.misc != null && morphoSpan.misc != "_") {
                                morphoSpan.misc!!.split("|")
                            } else {
                                emptyList()
                            }

                            // Sort by descending probability if probabilities are available
                            val sortedPairs = if (miscList.size == xposList.size && 
                                                 miscList.all { it.toDoubleOrNull() != null }) {
                                xposList.mapIndexed { index, xpos ->
                                    val certainty = miscList[index].toDoubleOrNull() ?: 0.0
                                    Pair(xpos, certainty)
                                }.sortedByDescending { it.second }
                            } else {
                                // If probabilities don't match, keep original order
                                xposList.mapIndexed { index, xpos ->
                                    val certainty = if (index < miscList.size) {
                                        miscList[index].toDoubleOrNull()
                                    } else {
                                        null
                                    }
                                    Pair(xpos, certainty)
                                }
                            }

                            sortedPairs.forEach { (xpos, certainty) ->
                                if (certainty != null && sortedPairs.size > 1) {
                                    val payload = kotlin.math.round(certainty * 255).toInt()
                                    tokenAnnotations.add(jsonString("$prefix/p:${xpos.escapeKrillValue()}\$<b>129<b>$payload"))
                                } else {
                                    tokenAnnotations.add(jsonString("$prefix/p:${xpos.escapeKrillValue()}"))
                                }
                            }
                        }

                        // Lemma - sorted by descending probability if probabilities are available
                        if (morphoSpan.lemma != null && morphoSpan.lemma != "_") {
                            val lemmaList = morphoSpan.lemma!!.split("|").distinct()
                            val miscList = if (morphoSpan.misc != null && morphoSpan.misc != "_") {
                                morphoSpan.misc!!.split("|")
                            } else {
                                emptyList()
                            }
                            
                            // Extract probabilities from misc (exclude Offset= parts)
                            val probabilities = miscList.filter { !it.startsWith("Offset=") }
                                .mapNotNull { it.toDoubleOrNull() }
                            
                            val sortedLemmas = if (probabilities.size == lemmaList.size) {
                                // Sort by descending probability
                                lemmaList.mapIndexed { index, lemma ->
                                    val certainty = probabilities.getOrNull(index) ?: 0.0
                                    Pair(lemma, certainty)
                                }.sortedByDescending { it.second }.map { it.first }
                            } else {
                                // If probabilities don't match, keep original order
                                lemmaList
                            }
                            
                            sortedLemmas.forEach { lemma ->
                                tokenAnnotations.add(jsonString("$prefix/l:${lemma.escapeKrillValue()}"))
                            }
                        }

                        // UPOS (skip for tree_tagger as it only has xpos)
                        if (morphoSpan.upos != null && morphoSpan.upos != "_" && foundry != "tree_tagger") {
                            tokenAnnotations.add(jsonString("$prefix/u:${morphoSpan.upos!!.escapeKrillValue()}"))
                        }
                    }

                    // Dependency relations
                    if (morphoSpan.head != null && morphoSpan.head != "_" && morphoSpan.deprel != null && morphoSpan.deprel != "_") {
                        // Head can be either an offset (e.g., "100-110") or a token index (e.g., "1")
                        val headStr = morphoSpan.head!!
                        val resolvedHeadIndex = if (headStr.contains("-")) {
                            // Offset format - resolve to token index
                            offsetToIndex[headStr]
                        } else {
                            // Already a token index (1-based CoNLL-U format)
                            val idx = headStr.toIntOrNull()
                            if (idx != null && idx > 0) idx - 1 else null  // Convert 1-based to 0-based
                        }

                        if (resolvedHeadIndex != null) {
                            // Regular dependency - outgoing edge to head
                            tokenAnnotations.add(jsonString(">:$prefix/d:${morphoSpan.deprel!!.escapeKrillValue()}\$<b>32<i>$resolvedHeadIndex"))
                        } else if (headStr == "0" || (headStr.contains("-") && headStr.startsWith("0-"))) {
                            // ROOT node - use incoming edge format with full span info
                            tokenAnnotations.add(jsonString("<:$prefix/d:${morphoSpan.deprel!!.escapeKrillValue()}\$<b>34<i>${token.from}<i>${token.to}<i>$index<i>1"))
                        }
                    }
                }
            }

            // Surface form (always last)
            tokenAnnotations.add(jsonString("s:${surfaceForm.escapeKrillValue()}"))

            result.add(jsonArray(tokenAnnotations))
        }

        return result
    }

    private fun shouldKeepTokenForKrill(text: NonBmpString, span: Span): Boolean {
        if (text.length == 0) return true
        val safeFrom = span.from.coerceIn(0, text.length)
        val safeTo = span.to.coerceIn(safeFrom, text.length)
        if (safeFrom >= safeTo) return false
        val surface = text.subSequence(safeFrom, safeTo).toString()
        val keep = surface.any { 
            it.isLetterOrDigit() || 
            it == '_' || 
            it.isSurrogate() || 
            Character.getType(it) == Character.OTHER_SYMBOL.toInt() 
        }
        return keep
    }

    // Extension functions for StructureSpan collections

    private fun StructureSpan.splitFoundryAndLayer(): Pair<String, String>? {
        val separatorIdx = layer.indexOf('/')
        if (separatorIdx <= 0 || separatorIdx == layer.length - 1) {
            return null
        }
        val foundry = layer.substring(0, separatorIdx)
        val descriptor = layer.substring(separatorIdx + 1)
        if (foundry.isBlank() || descriptor.isBlank()) {
            return null
        }
        return foundry to descriptor
    }

    private fun Collection<StructureSpan>.foundriesWithSentenceSpans(): Set<String> =
        this.mapNotNull { span ->
            val (foundry, descriptor) = span.splitFoundryAndLayer() ?: return@mapNotNull null
            if (descriptor == "s:s") foundry else null
        }.toSet()

    private fun Collection<StructureSpan>.foundriesWithConstituencySpans(): Set<String> =
        this.mapNotNull { span ->
            val (foundry, descriptor) = span.splitFoundryAndLayer() ?: return@mapNotNull null
            if (descriptor.startsWith("c:")) foundry else null
        }.toSet()

    private fun Collection<StructureSpan>.sentenceCountsByFoundry(): Map<String, Int> {
        val counts = mutableMapOf<String, Int>()
        this.forEach { span ->
            val (foundry, descriptor) = span.splitFoundryAndLayer() ?: return@forEach
            if (descriptor == "s:s") {
                counts[foundry] = counts.getOrDefault(foundry, 0) + 1
            }
        }
        return counts
    }

    // JSON utility functions for Krill output (no external JSON library dependency)

    private fun String.escapeJson(): String {
        val sb = StringBuilder()
        for (c in this) {
            when (c) {
                '\\' -> sb.append("\\\\")
                '"' -> sb.append("\\\"")
                '\b' -> sb.append("\\b")
                '\n' -> sb.append("\\n")
                '\r' -> sb.append("\\r")
                '\t' -> sb.append("\\t")
                else -> {
                    if (c < ' ') {
                        sb.append(String.format("\\u%04x", c.code))
                    } else {
                        sb.append(c)
                    }
                }
            }
        }
        return sb.toString()
    }

    // Escape special characters in Krill attribute values
    private fun String.escapeKrillAttribute(): String {
        return this.replace("#", "%23")
            .replace("$", "%24")
            .replace(":", "%3A")
            .replace("<", "%3C")
            .replace(">", "%3E")
    }

    // Escape special characters in Krill annotation values
    private fun String.escapeKrillValue(): String {
        return this.replace("#", "\\#")
            .replace("$", "\\$")
    }

    private fun jsonString(value: String): String = "\"${value.escapeJson()}\""

    private fun jsonArray(items: List<String>): String = items.joinToString(",", "[", "]")

    private fun jsonObject(pairs: List<Pair<String, String>>): String {
        return pairs.joinToString(",", "{", "}") { (key, value) ->
            "${jsonString(key)}:${value}"
        }
    }

    private fun String.urlEncode(): String {
        return java.net.URLEncoder.encode(this, "UTF-8")
    }
}
