package de.ids_mannheim.korapxmltools.formatters

import de.ids_mannheim.korapxmltools.KorapXmlTool
import java.util.SortedSet

/**
 * Surgical, lexical-level patching of existing Krill JSON documents, used when an
 * existing Krill tar is given as input and should be updated from KorAP-XML ZIPs
 * and/or stand-off metadata files ("merge mode").
 *
 * The patcher never re-serializes untouched parts of a document: it locates the
 * regions to change with a small JSON scanner and splices replacement text in,
 * so texts and annotations that are not affected by the update are preserved
 * byte-for-byte. This keeps the merge robust for tars produced by older versions
 * of this tool or by korapxml2krill (Perl).
 */
object KrillJsonPatcher {

    /** How a new field is merged into the existing "fields" array. */
    enum class FieldMode {
        /** Replace the existing field with the same (or alias) key, else append. */
        REPLACE_OR_APPEND,

        /** Only append if no field with the same (or alias) key exists. */
        APPEND_IF_MISSING
    }

    data class FieldPatch(val key: String, val json: String, val mode: FieldMode)

    // Metadata keys that were renamed (see KrillJsonGenerator.CORRECTED_FIELD_NAMES).
    // Replacing a field under one name must also remove its counterpart, so a tar
    // written with legacy names never ends up carrying both variants.
    private val FIELD_KEY_ALIASES = mapOf(
        "textClass" to "dmozDomain", "dmozDomain" to "textClass",
        "textDomain" to "idsColumn", "idsColumn" to "textDomain"
    )

    /**
     * Merge [patches] into the top-level "fields" array of [json].
     *
     * Existing fields keep their position; a REPLACE_OR_APPEND patch replaces the
     * first field whose key (or alias key) matches and removes later duplicates;
     * patches without a match are appended at the end in the given order.
     */
    fun patchFields(json: String, patches: List<FieldPatch>): String {
        if (patches.isEmpty()) return json
        val fieldsRegion = findTopLevelMemberValue(json, "fields")
            ?: throw IllegalArgumentException("No top-level \"fields\" array found in Krill JSON")
        val elements = parseArrayElements(json, fieldsRegion)
        val existingKeys = elements.map { extractMemberString(json, it, "key") }

        // Resolve which existing element index each replacement patch targets.
        val replacementByIndex = mutableMapOf<Int, FieldPatch>()
        val dropIndices = mutableSetOf<Int>()
        val toAppend = mutableListOf<FieldPatch>()
        patches.forEach { patch ->
            val targetKeys = setOf(patch.key, FIELD_KEY_ALIASES[patch.key] ?: patch.key)
            val matches = existingKeys.withIndex().filter { (_, k) -> k != null && k in targetKeys }
            when {
                matches.isEmpty() -> toAppend.add(patch)

                patch.mode == FieldMode.APPEND_IF_MISSING -> { /* exists: keep as is */ }

                else -> {
                    replacementByIndex[matches.first().index] = patch
                    // Remove later duplicates and alias-named variants of the same field
                    matches.drop(1).forEach { dropIndices.add(it.index) }
                }
            }
        }
        if (replacementByIndex.isEmpty() && dropIndices.isEmpty() && toAppend.isEmpty()) return json

        val newArray = StringBuilder()
        var first = true
        elements.forEachIndexed { idx, range ->
            if (idx in dropIndices) return@forEachIndexed
            if (!first) newArray.append(',')
            newArray.append(replacementByIndex[idx]?.json ?: json.substring(range.first, range.last + 1))
            first = false
        }
        toAppend.forEach { patch ->
            if (!first) newArray.append(',')
            newArray.append(patch.json)
            first = false
        }

        return json.substring(0, fieldsRegion.first + 1) + newArray +
            json.substring(fieldsRegion.last)
    }

    /**
     * Extract the value of the top-level "textSigle" field from the "fields" array,
     * e.g. "REI/RBR/00473", or null if absent. Used by merge mode to recover the
     * text id of a tar entry independent of its (lossily normalized) file name.
     */
    fun extractTextSigle(json: String): String? {
        val fieldsRegion = findTopLevelMemberValue(json, "fields") ?: return null
        parseArrayElements(json, fieldsRegion).forEach { el ->
            if (extractMemberString(json, el, "key") == "textSigle") {
                return extractMemberString(json, el, "value")
            }
        }
        return null
    }

    // ------------------------------------------------------------------
    // Foundry (token stream) patching
    // ------------------------------------------------------------------

    /**
     * Replace or add annotation foundries in an existing Krill JSON document.
     *
     * [textData] carries the new annotations collected from KorAP-XML ZIP entries
     * (morpho/dependency/sentences/constituency) for this text. Every foundry it
     * contains is treated as authoritative: all existing stream annotations of
     * that foundry are removed and the new ones inserted at the positions full
     * generation would put them; layerInfos and foundries summaries are updated.
     * Token offsets come from the existing stream, so the original base ZIP is
     * not needed. Annotations whose offsets match no stream token (tokenization
     * drift, filtered non-word tokens) are silently skipped, like in full
     * generation.
     */
    fun patchFoundries(json: String, textData: KrillJsonGenerator.KrillTextData): String {
        val foundries = (textData.morphoByFoundry.keys +
            textData.sentencesCollectedByFoundry +
            textData.constituencyCollectedByFoundry)
            .filterNot { it == "base" || it == "dereko" }
            .toSortedSet()
        if (foundries.isEmpty()) return json

        val dataRange = findTopLevelMemberValue(json, "data")
            ?: throw IllegalArgumentException("No top-level \"data\" object found in Krill JSON")
        val streamRange = findMemberValue(json, "stream", dataRange.first)
            ?: throw IllegalArgumentException("No \"stream\" array found in Krill JSON data")
        val layerInfosRange = findMemberValue(json, "layerInfos", dataRange.first)
        val foundriesRange = findMemberValue(json, "foundries", dataRange.first)

        // Parse the stream into per-token lists of raw (escaped, unquoted) annotation strings
        val tokenArrays: MutableList<MutableList<String>> = parseArrayElements(json, streamRange).map { tokRange ->
            parseArrayElements(json, tokRange).map { el ->
                require(json[el.first] == '"') { "Non-string stream annotation at offset ${el.first}" }
                json.substring(el.first + 1, el.last)
            }.toMutableList()
        }.toMutableList()

        // Token offsets from the existing stream ("_<i>$<i>from<i>to")
        val offsetRegex = Regex("""^_\d+\$<i>(\d+)<i>(\d+)$""")
        val tokens = tokenArrays.mapIndexed { idx, anns ->
            val m = anns.firstNotNullOfOrNull { offsetRegex.matchEntire(it) }
                ?: throw IllegalArgumentException("Stream token $idx has no offset annotation")
            KorapXmlTool.Span(m.groupValues[1].toInt(), m.groupValues[2].toInt())
        }
        val offsetToIndex = HashMap<String, Int>(tokens.size * 2)
        tokens.forEachIndexed { index, t -> offsetToIndex["${t.from}-${t.to}"] = index }

        var layerInfoTokens = layerInfosRange?.let {
            unescapeJsonString(json.substring(it.first + 1, it.last)).split(" ").filter { t -> t.isNotEmpty() }
        } ?: emptyList()
        var foundriesTokens = foundriesRange?.let {
            unescapeJsonString(json.substring(it.first + 1, it.last)).split(" ").filter { t -> t.isNotEmpty() }
        } ?: emptyList()

        foundries.forEach { foundry ->
            val contribution = buildContribution(foundry, textData, tokens, offsetToIndex)
            removeFoundryAnnotations(tokenArrays, contribution.names)
            insertFoundryAnnotations(tokenArrays, contribution)
            layerInfoTokens = rebuildLayerInfos(layerInfoTokens, contribution)
            foundriesTokens = rebuildFoundries(foundriesTokens, contribution)
        }

        // Re-serialize the three patched regions, splicing right-to-left so offsets stay valid
        val replacements = mutableListOf<Pair<IntRange, String>>()
        replacements.add(streamRange to tokenArrays.joinToString(",", "[", "]") { anns ->
            anns.joinToString(",", "[", "]") { "\"$it\"" }
        })
        layerInfosRange?.let { replacements.add(it to KrillJsonGenerator.quoteJson(layerInfoTokens.joinToString(" "))) }
        foundriesRange?.let { replacements.add(it to KrillJsonGenerator.quoteJson(foundriesTokens.joinToString(" "))) }
        replacements.sortByDescending { it.first.first }

        val sb = StringBuilder(json)
        replacements.forEach { (range, text) ->
            sb.replace(range.first, range.last + 1, text)
        }
        return sb.toString()
    }

    /** Everything one foundry contributes to a text's stream and summary strings. */
    private class FoundryContribution(
        val foundry: String,
        val prefix: String,
        val fullName: String,
        /** All name variants whose existing annotations must be removed. */
        val names: Set<String>,
        val sentenceCount: Int,
        /** Resolved structural spans (sentences/constituency), keyed by start token. */
        val spansByToken: Map<Int, List<KrillJsonGenerator.StructureSpan>>,
        val hasConstituency: Boolean,
        /** Inverse dependency annotations, keyed by head token: (sortKey, raw annotation). */
        val inverseByToken: Map<Int, List<Pair<String, String>>>,
        /** Per-token morpho/dependency block (raw annotation strings). */
        val morphoByToken: Map<Int, List<String>>,
        /** layerInfos descriptors like "p=tokens" for the morpho prefix. */
        val morphoLayers: SortedSet<String>
    )

    private fun buildContribution(
        foundry: String,
        textData: KrillJsonGenerator.KrillTextData,
        tokens: List<KorapXmlTool.Span>,
        offsetToIndex: Map<String, Int>
    ): FoundryContribution {
        val prefix = KrillJsonGenerator.foundryPrefix(foundry) ?: foundry
        val fullName = KrillJsonGenerator.foundryFullNameForPrefix(prefix)
        val morphoSpans = textData.morphoByFoundry[foundry]

        // Structural spans of this foundry (sentences "f/s:s", constituency "f/c:X")
        val spans = textData.structureSpans
            .filter { it.layer.startsWith("$foundry/") }
            .map { KrillJsonGenerator.resolveStructureSpanTokenRange(it, tokens) }
            .filter { it.tokenFrom >= 0 }
        val spansByToken = spans.groupBy { it.tokenFrom }
            .mapValues { (_, list) ->
                list.sortedWith(compareByDescending<KrillJsonGenerator.StructureSpan> { it.depth }.thenBy { it.layer })
            }
        val sentenceCount = spans.count { it.layer == "$foundry/s:s" }
        val hasConstituency = spans.any { it.layer.substringAfter('/').startsWith("c:") }

        val morphoByToken = HashMap<Int, List<String>>()
        val inverseByToken = HashMap<Int, MutableList<Pair<String, String>>>()
        if (morphoSpans != null) {
            tokens.forEachIndexed { index, token ->
                val spanKey = "${token.from}-${token.to}"
                val morphoSpan = morphoSpans[spanKey] ?: return@forEachIndexed
                val anns = KrillJsonGenerator.morphoAnnotationsForToken(
                    prefix, foundry, morphoSpan, token, index, offsetToIndex
                )
                if (anns.isNotEmpty()) {
                    morphoByToken[index] = anns
                }
                // Inverse dependency edges pointing at this token's head
                val headStr = morphoSpan.head
                val deprel = morphoSpan.deprel
                if (headStr != null && headStr != "_" && deprel != null && deprel != "_" &&
                    !KrillJsonGenerator.isRootHead(headStr)
                ) {
                    val headIndex = KrillJsonGenerator.resolveHeadIndex(headStr, offsetToIndex)
                    if (headIndex != null) {
                        inverseByToken.getOrPut(headIndex) { mutableListOf() }
                            .add("$prefix/$deprel" to KrillJsonGenerator.inverseDependencyAnnotation(prefix, deprel, index))
                    }
                }
            }
            inverseByToken.values.forEach { list -> list.sortBy { it.first } }
        }

        return FoundryContribution(
            foundry = foundry,
            prefix = prefix,
            fullName = fullName,
            names = setOf(foundry, prefix, fullName),
            sentenceCount = sentenceCount,
            spansByToken = spansByToken,
            hasConstituency = hasConstituency,
            inverseByToken = inverseByToken,
            morphoByToken = morphoByToken,
            morphoLayers = KrillJsonGenerator.computeFoundryLayers(foundry, morphoSpans?.values)
        )
    }

    // Position classes of stream annotations, in the order full generation emits them
    private const val CLS_COUNTS = 0     // "-:..." (token 0 only)
    private const val CLS_SPANS = 1      // "<>:..." and their "@:..." attributes
    private const val CLS_OFFSET = 2     // "_<i>$<i>f<i>t"
    private const val CLS_LOWER = 3      // "i:..."
    private const val CLS_NONWORD = 4    // "base/p:_"
    private const val CLS_INVDEP = 5     // "<:X/d:...$<b>32<i>n"
    private const val CLS_MORPHO = 6     // foundry blocks: "X/...", ">:X/...", root "<:...<b>34..."
    private const val CLS_SURFACE = 7    // "s:..."

    private fun classify(raw: String): Int = when {
        raw.startsWith("-:") -> CLS_COUNTS
        raw.startsWith("<>:") || raw.startsWith("@:") -> CLS_SPANS
        raw.startsWith("_") -> CLS_OFFSET
        raw.startsWith("i:") -> CLS_LOWER
        raw == "base/p:_" -> CLS_NONWORD
        raw.startsWith("<:") && raw.contains("\$<b>32") -> CLS_INVDEP
        raw.startsWith("s:") -> CLS_SURFACE
        else -> CLS_MORPHO
    }

    /** Foundry name variant an annotation belongs to, or null (base/structural/bookkeeping). */
    private fun annotationOwner(raw: String): String? {
        val body = when {
            raw.startsWith("<>:") -> raw.substring(3)
            raw.startsWith(">:") || raw.startsWith("-:") || raw.startsWith("@:") -> raw.substring(2)
            raw.startsWith("<:") -> raw.substring(2)
            else -> raw
        }
        val slash = body.indexOf('/')
        if (slash <= 0) return null
        return body.substring(0, slash)
    }

    private fun removeFoundryAnnotations(tokenArrays: MutableList<MutableList<String>>, names: Set<String>) {
        tokenArrays.forEach { anns ->
            anns.removeAll { raw ->
                classify(raw) != CLS_NONWORD && annotationOwner(raw) in names
            }
        }
    }

    private fun insertFoundryAnnotations(
        tokenArrays: MutableList<MutableList<String>>,
        contribution: FoundryContribution
    ) {
        tokenArrays.forEachIndexed { index, anns ->
            // Sentence count at token 0, among "-:X/sentences" sorted by foundry, before "-:tokens"
            if (index == 0 && contribution.sentenceCount > 0) {
                val countAnn = "-:${contribution.foundry}/sentences\$<i>${contribution.sentenceCount}"
                var pos = 0
                while (pos < anns.size && classify(anns[pos]) == CLS_COUNTS) {
                    val raw = anns[pos]
                    val owner = annotationOwner(raw)
                    if (raw.startsWith("-:tokens") || (owner != null && owner != "base" && owner > contribution.foundry)) break
                    pos++
                }
                anns.add(pos, escapeAnnotation(countAnn))
            }

            // Structural spans among "<>:" entries, ordered by (depth desc, layer)
            contribution.spansByToken[index]?.forEach { span ->
                val ann = KrillJsonGenerator.structureSpanAnnotation(span)
                var pos = anns.indexOfFirst { classify(it) == CLS_SPANS }
                if (pos < 0) {
                    // No spans at this token yet: spans go right before the offset annotation
                    pos = anns.indexOfFirst { classify(it) == CLS_OFFSET }
                    if (pos < 0) pos = anns.size
                } else {
                    while (pos < anns.size && classify(anns[pos]) == CLS_SPANS) {
                        val existing = parseSpanOrder(anns[pos])
                        if (existing != null &&
                            (existing.first < span.depth ||
                                (existing.first == span.depth && existing.second > span.layer))
                        ) break
                        pos++
                    }
                }
                anns.add(pos, escapeAnnotation(ann))
            }

            // Inverse dependency edges, sorted by "prefix/deprel" among existing <b>32 edges
            contribution.inverseByToken[index]?.forEach { (sortKey, ann) ->
                var pos = anns.indexOfFirst { classify(it) == CLS_INVDEP }
                if (pos < 0) {
                    // Zone is empty: it sits after i:/base/p:_ and before the morpho blocks
                    pos = anns.indexOfFirst { classify(it) == CLS_MORPHO || classify(it) == CLS_SURFACE }
                    if (pos < 0) pos = anns.size
                } else {
                    while (pos < anns.size && classify(anns[pos]) == CLS_INVDEP) {
                        val existingKey = parseInverseDepKey(anns[pos])
                        if (existingKey != null && existingKey > sortKey) break
                        pos++
                    }
                }
                anns.add(pos, escapeAnnotation(ann))
            }

            // The foundry's morpho/dependency block, between blocks sorted by foundry name
            contribution.morphoByToken[index]?.let { block ->
                var pos = -1
                var i = 0
                while (i < anns.size) {
                    val cls = classify(anns[i])
                    if (cls == CLS_SURFACE) {
                        if (pos < 0) pos = i
                        break
                    }
                    if (cls == CLS_MORPHO) {
                        val owner = annotationOwner(anns[i])
                        val ownerKey = owner?.let { foundrySortKey(it) }
                        if (ownerKey != null && ownerKey > contribution.foundry) {
                            pos = i
                            break
                        }
                    }
                    i++
                }
                if (pos < 0) pos = anns.size
                anns.addAll(pos, block.map { escapeAnnotation(it) })
            }
        }
    }

    /** Sort key of a stream-annotation owner: map layer prefixes back to foundry names. */
    private fun foundrySortKey(owner: String): String = when (owner) {
        "tt", "treetagger" -> "tree_tagger"
        else -> owner
    }

    /** (depth, layer) of an existing "<>:" span annotation, for ordered insertion. */
    private fun parseSpanOrder(raw: String): Pair<Int, String>? {
        val m = Regex("""^<>:([^$]+)\$<b>64(?:<i>-?\d+){3}<b>(\d+)""").find(raw) ?: return null
        return m.groupValues[2].toInt() to m.groupValues[1]
    }

    /** "foundry/deprel" sort key of an existing inverse dependency annotation. */
    private fun parseInverseDepKey(raw: String): String? {
        val m = Regex("""^<:([^/]+)/d:([^$]*)\$""").find(raw) ?: return null
        return "${m.groupValues[1]}/${m.groupValues[2]}"
    }

    /** JSON-escape a raw annotation exactly like the generator (content without quotes). */
    private fun escapeAnnotation(ann: String): String {
        val quoted = KrillJsonGenerator.quoteJson(ann)
        return quoted.substring(1, quoted.length - 1)
    }

    private fun rebuildLayerInfos(existing: List<String>, c: FoundryContribution): List<String> {
        val kept = existing.filterNot { it.substringBefore('/') in c.names }
        val added = mutableListOf<String>()
        if (c.sentenceCount > 0) added.add("${c.foundry}/s=spans")
        if (c.hasConstituency) added.add("${c.foundry}/c=spans")
        c.morphoLayers.forEach { added.add("${c.prefix}/$it") }
        return (kept + added).sortedWith(compareBy({ layerInfoRank(it) }, { it.substringBefore('/') }, { it.substringAfter('/') }))
    }

    private fun layerInfoRank(token: String): Int = when {
        token == "dereko/s=spans" -> 0
        token == "base/p=tokens" -> 1
        token.endsWith("/s=spans") -> 2
        token.endsWith("/c=spans") -> 3
        else -> 4
    }

    private fun rebuildFoundries(existing: List<String>, c: FoundryContribution): List<String> {
        val kept = existing.filterNot { it.substringBefore('/') in c.names }

        // Reconstruct the sets the generator derives its ordering from
        val sentFoundries = kept.filter { it.endsWith("/sentences") }.map { it.substringBefore('/') }.toSortedSet()
        val constitFoundries = kept.filter { it.endsWith("/structure") && it.substringBefore('/') != "dereko" }
            .map { it.substringBefore('/') }.toSortedSet()
        val annLayers = sortedMapOf<String, SortedSet<String>>()
        kept.forEach {
            val name = it.substringBefore('/')
            val layer = it.substringAfter('/', "")
            if (layer == "morpho" || layer == "dependency") {
                annLayers.getOrPut(name) { sortedSetOf() }.add(layer)
            }
        }

        if (c.sentenceCount > 0) sentFoundries.add(c.foundry)
        if (c.hasConstituency) constitFoundries.add(c.foundry)
        if (c.morphoLayers.isNotEmpty()) {
            val layers = annLayers.getOrPut(c.fullName) { sortedSetOf() }
            layers.clear()
            if (c.morphoLayers.any { it == "d=rels" }) layers.add("dependency")
            if (c.morphoLayers.any { it != "d=rels" }) layers.add("morpho")
        }

        val result = mutableListOf<String>()
        // dereko block stays as-is (order is fixed)
        kept.filter { it == "dereko" || it.startsWith("dereko/") }.forEach { result.add(it) }
        // structure-advertised foundries: bare name, X/sentences, X/structure
        (sentFoundries + constitFoundries).toSortedSet().forEach { f ->
            if (!result.contains(f)) result.add(f)
            if (f in sentFoundries) {
                val e = "$f/sentences"
                if (!result.contains(e)) result.add(e)
            }
            if (f in constitFoundries) {
                val e = "$f/structure"
                if (!result.contains(e)) result.add(e)
            }
        }
        // annotation foundries, sorted like the generator (by layer prefix)
        annLayers.keys.sortedBy { name -> if (name == "treetagger") "tt" else name }.forEach { name ->
            result.add(name)
            annLayers[name]!!.forEach { layer ->
                val e = "$name/$layer"
                if (!result.contains(e)) result.add(e)
            }
        }
        return result
    }

    // ------------------------------------------------------------------
    // Minimal JSON scanning utilities.
    //
    // These operate directly on the document string and return index ranges
    // (inclusive first, inclusive last) so callers can splice text without
    // re-serializing what they don't touch.
    // ------------------------------------------------------------------

    /** Index just past a JSON string that starts at [start] (which must be '"'). */
    private fun skipString(s: String, start: Int): Int {
        var i = start + 1
        while (i < s.length) {
            when (s[i]) {
                '\\' -> i += 2
                '"' -> return i + 1
                else -> i++
            }
        }
        throw IllegalArgumentException("Unterminated JSON string at offset $start")
    }

    /** Index just past the JSON value starting at [start] (skips leading whitespace). */
    private fun skipValue(s: String, start: Int): Int {
        var i = skipWhitespace(s, start)
        return when (s[i]) {
            '"' -> skipString(s, i)
            '{', '[' -> {
                val open = s[i]
                val close = if (open == '{') '}' else ']'
                var depth = 0
                while (i < s.length) {
                    when (s[i]) {
                        '"' -> {
                            i = skipString(s, i)
                            continue
                        }
                        open -> depth++
                        close -> {
                            depth--
                            if (depth == 0) return i + 1
                        }
                    }
                    i++
                }
                throw IllegalArgumentException("Unbalanced JSON value at offset $start")
            }
            else -> {  // number, true, false, null
                while (i < s.length && s[i] !in charArrayOf(',', '}', ']') && !s[i].isWhitespace()) i++
                i
            }
        }
    }

    private fun skipWhitespace(s: String, start: Int): Int {
        var i = start
        while (i < s.length && s[i].isWhitespace()) i++
        return i
    }

    /**
     * Find the value of member [key] in the object starting at [objStart]
     * (default: the root object). Returns the value's inclusive index range,
     * or null if the key is not present at this object's top level.
     */
    fun findMemberValue(s: String, key: String, objStart: Int = 0): IntRange? {
        var i = skipWhitespace(s, objStart)
        require(i < s.length && s[i] == '{') { "Expected object at offset $objStart" }
        i++
        while (i < s.length) {
            i = skipWhitespace(s, i)
            if (s[i] == '}') return null
            require(s[i] == '"') { "Expected member key at offset $i" }
            val keyEnd = skipString(s, i)
            val memberKey = unescapeJsonString(s.substring(i + 1, keyEnd - 1))
            i = skipWhitespace(s, keyEnd)
            require(s[i] == ':') { "Expected ':' at offset $i" }
            i = skipWhitespace(s, i + 1)
            val valueEnd = skipValue(s, i)
            if (memberKey == key) return IntRange(i, valueEnd - 1)
            i = skipWhitespace(s, valueEnd)
            if (i < s.length && s[i] == ',') i++ else if (i < s.length && s[i] == '}') return null
        }
        return null
    }

    /** Find the value range of a member of the root object. */
    fun findTopLevelMemberValue(s: String, key: String): IntRange? = findMemberValue(s, key, 0)

    /** Inclusive ranges of the elements of the array spanning [arrayRange]. */
    fun parseArrayElements(s: String, arrayRange: IntRange): List<IntRange> {
        var i = skipWhitespace(s, arrayRange.first)
        require(s[i] == '[') { "Expected array at offset ${arrayRange.first}" }
        i = skipWhitespace(s, i + 1)
        val elements = mutableListOf<IntRange>()
        if (i <= arrayRange.last && s[i] == ']') return elements
        while (i <= arrayRange.last) {
            val end = skipValue(s, i)
            elements.add(IntRange(skipWhitespace(s, i), end - 1))
            i = skipWhitespace(s, end)
            if (i > arrayRange.last || s[i] == ']') break
            require(s[i] == ',') { "Expected ',' in array at offset $i" }
            i = skipWhitespace(s, i + 1)
        }
        return elements
    }

    /** The string value of member [key] of the object at [objRange], unescaped; null if absent or not a string. */
    fun extractMemberString(s: String, objRange: IntRange, key: String): String? {
        val valueRange = findMemberValue(s, key, objRange.first) ?: return null
        if (s[valueRange.first] != '"') return null
        return unescapeJsonString(s.substring(valueRange.first + 1, valueRange.last))
    }

    fun unescapeJsonString(escaped: String): String {
        if ('\\' !in escaped) return escaped
        val sb = StringBuilder(escaped.length)
        var i = 0
        while (i < escaped.length) {
            val c = escaped[i]
            if (c != '\\') {
                sb.append(c); i++; continue
            }
            i++
            when (val e = escaped[i]) {
                '"', '\\', '/' -> sb.append(e)
                'b' -> sb.append('\b')
                'f' -> sb.append('\u000C')
                'n' -> sb.append('\n')
                'r' -> sb.append('\r')
                't' -> sb.append('\t')
                'u' -> {
                    sb.append(escaped.substring(i + 1, i + 5).toInt(16).toChar())
                    i += 4
                }
                else -> throw IllegalArgumentException("Invalid JSON escape '\\$e'")
            }
            i++
        }
        return sb.toString()
    }
}
