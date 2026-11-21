package de.ids_mannheim.korapxmltools

/**
 * A named-entity span covering character offsets [from]..[to] (matching token boundaries)
 * with one or more bare category tags (e.g. ["PER"], ["ORG", "LOC"]).
 */
data class NamedEntitySpan(
    val from: Int,
    val to: Int,
    val tags: List<String>
)

/**
 * A named-entity span resolved against the Krill token stream, ready to be emitted as both
 * a span annotation (`<>:foundry/ne:TAG`) and bare per-token annotations (`foundry/ne:TAG`).
 *
 * [tokenTo] is exclusive (one past the last token index), matching [KrillJsonGenerator.StructureSpan].
 */
data class ResolvedNamedEntitySpan(
    val from: Int,
    val to: Int,
    val tokenFrom: Int,
    val tokenTo: Int,
    val tag: String,
    val depth: Int
)

/**
 * Shared normalization, BIO/IO-to-span collapsing, and token-range resolution for
 * named-entity annotations across CoreNLP, CoNLL-U Plus, KorAP-XML, and Krill.
 *
 * Standard coarse class names (`PER`, `LOC`, `ORG`, `MISC`) are used without `B-`/`I-`
 * prefixes and without `O` tags: entity boundaries are represented directly by multi-token
 * spans (`from`..`to`), while Krill additionally projects the bare class onto each covered
 * token so both `<foundry/ne=PER>` and `[foundry/ne=PER]` queries work cleanly.
 */
object NamedEntities {

    enum class Boundary { BEGIN, CONTINUE, SINGLE, UNMARKED }

    data class ParsedTag(
        val boundary: Boundary,
        val label: String,
        val closesAfter: Boolean = false
    )

    private val NORMALIZED_TAG_MAP = mapOf(
        // CoreNLP verbose English class names -> short CoNLL/ParlaMint/UNER roots
        "PERSON" to "PER",
        "LOCATION" to "LOC",
        "ORGANIZATION" to "ORG",
        "MISC" to "MISC",
        // GermEval 2014 coarse class + concatenated part/deriv suffixes -> CLASS[:specifier]
        "OTH" to "MISC",
        "PERpart" to "PER:part",
        "PERderiv" to "PER:deriv",
        "LOCpart" to "LOC:part",
        "LOCderiv" to "LOC:deriv",
        "ORGpart" to "ORG:part",
        "ORGderiv" to "ORG:deriv",
        "OTHpart" to "MISC:part",
        "OTHderiv" to "MISC:deriv",
        "MISCpart" to "MISC:part",
        "MISCderiv" to "MISC:deriv"
    )

    private val BIO_PREFIX_REGEX = Regex("^([BIESLU])-(.+)$")

    /**
     * Normalize coarse NER class names and GermEval suffixes to prefix-first `CLASS[:specifier]`
     * form (`PER`, `LOC`, `ORG`, `MISC`, `PER:part`, `LOC:deriv`, etc.). Other tags (e.g. NKJP's
     * `persName:forename`, `placeName:country`, or OntoNotes' `GPE`) are preserved verbatim.
     */
    fun normalizeBaseTag(tag: String): String {
        val trimmed = tag.trim()
        NORMALIZED_TAG_MAP[trimmed]?.let { return it }
        // Normalize coarse root before ':' if present (e.g. OTH:part -> MISC:part, PERSON:forename -> PER:forename)
        val colon = trimmed.indexOf(':')
        if (colon > 0) {
            val root = trimmed.substring(0, colon)
            val mappedRoot = NORMALIZED_TAG_MAP[root]
            if (mappedRoot != null && mappedRoot != root) {
                return mappedRoot + trimmed.substring(colon)
            }
        }
        return trimmed
    }

    /**
     * Parse one raw NER token tag (BIO, BILOU/BIOES, IO, or bare) into its boundary role
     * and normalized bare class name. Returns null for outside/empty tags (`O`, `_`, blank).
     */
    fun parseTag(raw: String): ParsedTag? {
        val trimmed = raw.trim()
        if (trimmed.isEmpty() || trimmed == "_" || trimmed == "O") return null
        val m = BIO_PREFIX_REGEX.matchEntire(trimmed)
        if (m != null) {
            val prefix = m.groupValues[1]
            val label = normalizeBaseTag(m.groupValues[2])
            if (label.isEmpty() || label == "_" || label == "O") return null
            return when (prefix) {
                "B" -> ParsedTag(Boundary.BEGIN, label)
                "I" -> ParsedTag(Boundary.CONTINUE, label)
                "E", "L" -> ParsedTag(Boundary.CONTINUE, label, closesAfter = true)
                "S", "U" -> ParsedTag(Boundary.SINGLE, label, closesAfter = true)
                else -> ParsedTag(Boundary.UNMARKED, label)
            }
        }
        val label = normalizeBaseTag(trimmed)
        if (label.isEmpty() || label == "_" || label == "O") return null
        return ParsedTag(Boundary.UNMARKED, label)
    }

    /** True if [raw] (or any `|`-separated alternative in it) carries a BIO/BILOU/BIOES prefix. */
    fun hasBioPrefix(raw: String): Boolean =
        raw.split("|").any { BIO_PREFIX_REGEX.matches(it.trim()) }

    /**
     * True if [raw] (or any `|`-separated alternative in it) is an unnormalized legacy tag
     * (such as CoreNLP's `PERSON`/`LOCATION`/`ORGANIZATION` or GermEval's `OTH`/`*part`/`*deriv`)
     * that cannot occur in the current span-based `ne.xml` format.
     */
    fun isUnnormalizedLegacyTag(raw: String): Boolean =
        raw.split("|").any { part ->
            val trimmed = part.trim()
            trimmed.isNotEmpty() && trimmed != "_" && trimmed != "O" &&
                !BIO_PREFIX_REGEX.matches(trimmed) &&
                normalizeBaseTag(trimmed) != trimmed
        }

    /**
     * Collapse per-token NER tags (BIO, BILOU/BIOES, IO, or bare, optionally with `|`-separated
     * alternatives or nested labels, including nested entities of the same class) over an ordered
     * list of ALL tokens into multi-token [NamedEntitySpan]s with bare normalized tags.
     *
     * Untagged tokens (`null`, `_`, `O`) and [sentences] boundaries always close active spans,
     * so separated entities never merge across gaps or sentences.
     */
    fun collapseTokenTagsToSpans(
        tokens: List<KorapXmlTool.Span>,
        tokenTags: (KorapXmlTool.Span) -> String?,
        sentences: Array<KorapXmlTool.Span>? = null
    ): List<NamedEntitySpan> {
        if (tokens.isEmpty()) return emptyList()

        data class ActiveEntity(val from: Int, var to: Int, val label: String, val order: Int)
        data class EmittedEntity(val from: Int, val to: Int, val label: String, val order: Int)

        // Stack per class (bottom = outermost, top = innermost) so nested entities of the same class work
        val active = LinkedHashMap<String, ArrayDeque<ActiveEntity>>()
        val emitted = mutableListOf<EmittedEntity>()
        var nextOrder = 0

        fun flushAll() {
            for (stack in active.values) {
                for (ent in stack) {
                    emitted.add(EmittedEntity(ent.from, ent.to, ent.label, ent.order))
                }
            }
            active.clear()
        }

        val sortedSentences = sentences?.sortedBy { it.from }
        var sentenceIdx = 0
        var lastSentenceIdx = -1

        for (token in tokens) {
            if (sortedSentences != null && sortedSentences.isNotEmpty()) {
                while (sentenceIdx < sortedSentences.size && sortedSentences[sentenceIdx].to <= token.from) {
                    sentenceIdx++
                }
                val currentSentenceIdx = if (sentenceIdx < sortedSentences.size &&
                    token.from >= sortedSentences[sentenceIdx].from &&
                    token.to <= sortedSentences[sentenceIdx].to
                ) sentenceIdx else -1
                if (lastSentenceIdx != -1 && currentSentenceIdx != lastSentenceIdx) {
                    flushAll()
                }
                lastSentenceIdx = currentSentenceIdx
            }

            val raw = tokenTags(token)
            val parsed = if (raw.isNullOrBlank() || raw == "_") {
                emptyList()
            } else {
                raw.split("|").mapNotNull { parseTag(it) }
            }

            if (parsed.isEmpty()) {
                flushAll()
                continue
            }

            val parsedByLabel = LinkedHashMap<String, MutableList<ParsedTag>>()
            for (tag in parsed) {
                parsedByLabel.getOrPut(tag.label) { mutableListOf() }.add(tag)
            }

            // Any active entity whose label does not appear on this token ends at the previous token
            val toRemove = active.keys.filter { it !in parsedByLabel }
            for (label in toRemove) {
                val stack = active.remove(label)!!
                for (ent in stack) {
                    emitted.add(EmittedEntity(ent.from, ent.to, ent.label, ent.order))
                }
            }

            for ((label, tagsForLabel) in parsedByLabel) {
                val stack = active.getOrPut(label) { ArrayDeque() }
                val continuations = tagsForLabel.filter {
                    it.boundary == Boundary.CONTINUE || it.boundary == Boundary.UNMARKED
                }
                val begins = tagsForLabel.filter { it.boundary == Boundary.BEGIN }
                val singles = tagsForLabel.filter { it.boundary == Boundary.SINGLE }

                // Close innermost open entities that are not continued on this token
                // (e.g. a new adjacent B-PER closes the open PER; a single I-ORG closes an inner ORG)
                while (stack.size > continuations.size) {
                    val closed = stack.removeLast()
                    emitted.add(EmittedEntity(closed.from, closed.to, closed.label, closed.order))
                }

                // Extend all remaining open entities, closing innermost ones for E-/L- tags
                val existingCount = stack.size
                for (ent in stack) {
                    ent.to = token.to
                }
                var closingMatched = 0
                for (i in 0 until existingCount) {
                    if (continuations[i].closesAfter) closingMatched++
                }
                repeat(closingMatched) {
                    val closed = stack.removeLast()
                    emitted.add(EmittedEntity(closed.from, closed.to, closed.label, closed.order))
                }

                // Any extra continuations beyond open entities start new entities (IO/bare/orphan E-)
                for (i in existingCount until continuations.size) {
                    val cont = continuations[i]
                    if (cont.closesAfter) {
                        emitted.add(EmittedEntity(token.from, token.to, label, nextOrder++))
                    } else {
                        stack.addLast(ActiveEntity(token.from, token.to, label, nextOrder++))
                    }
                }

                for (ignored in begins) {
                    stack.addLast(ActiveEntity(token.from, token.to, label, nextOrder++))
                }
                for (ignored in singles) {
                    emitted.add(EmittedEntity(token.from, token.to, label, nextOrder++))
                }

                if (stack.isEmpty()) {
                    active.remove(label)
                }
            }
        }
        flushAll()

        if (emitted.isEmpty()) return emptyList()

        // Group identical (from, to) spans preserving tag order, sorted by (from asc, to desc)
        val grouped = LinkedHashMap<Pair<Int, Int>, MutableList<String>>()
        emitted.sortedWith(compareBy<EmittedEntity> { it.from }.thenByDescending { it.to }.thenBy { it.order })
            .forEach { ent ->
                val list = grouped.getOrPut(ent.from to ent.to) { mutableListOf() }
                if (ent.label !in list) list.add(ent.label)
            }

        return grouped.entries.map { (range, tags) ->
            NamedEntitySpan(range.first, range.second, tags)
        }
    }

    /**
     * Resolve raw [NamedEntitySpan]s (from one or more `ne.xml` files of a single foundry)
     * against the Krill [tokens] list, returning deduplicated [ResolvedNamedEntitySpan]s with
     * nesting depths computed.
     *
     * Accepts both multi-token bare-tag spans (the current `ne.xml` format) and legacy
     * per-token files — both BIO/IO (`B-PER`/`I-PER`, `I-LOC`) and unnormalized raw CoreNLP /
     * GermEval tags (`PERSON`, `LOCATION`, `ORGANIZATION`, `OTH`, `*part`, `*deriv`) —
     * collapsing the latter into multi-token spans automatically. (A legacy file whose only
     * entities in a text are bare `MISC` tags cannot be distinguished from the current format.)
     */
    fun resolveForKrill(
        rawSpans: List<NamedEntitySpan>,
        tokens: List<KorapXmlTool.Span>,
        sentences: Array<KorapXmlTool.Span>? = null,
        sortAlternatives: Boolean = false
    ): List<ResolvedNamedEntitySpan> {
        if (rawSpans.isEmpty() || tokens.isEmpty()) return emptyList()

        val hasAnyLegacyPerToken = rawSpans.any { span ->
            span.tags.any { hasBioPrefix(it) || isUnnormalizedLegacyTag(it) }
        }
        val normalizedSpans: List<NamedEntitySpan> = if (hasAnyLegacyPerToken) {
            // Separate single-token spans (to collapse across adjacent tokens) from any
            // already multi-token spans that might coexist in the same layer.
            val tokenOffsets = HashSet<Pair<Int, Int>>(tokens.size * 2)
            tokens.forEach { tokenOffsets.add(it.from to it.to) }

            val perTokenByOffset = HashMap<Pair<Int, Int>, MutableList<String>>()
            val alreadyMultiToken = mutableListOf<NamedEntitySpan>()
            for (span in rawSpans) {
                if ((span.from to span.to) in tokenOffsets) {
                    val list = perTokenByOffset.getOrPut(span.from to span.to) { mutableListOf() }
                    span.tags.forEach { t -> if (t !in list) list.add(t) }
                } else {
                    val clean = span.tags.mapNotNull { parseTag(it)?.label }.distinct()
                    if (clean.isNotEmpty()) {
                        alreadyMultiToken.add(NamedEntitySpan(span.from, span.to, clean))
                    }
                }
            }
            val collapsed = collapseTokenTagsToSpans(
                tokens = tokens,
                tokenTags = { tok ->
                    perTokenByOffset[tok.from to tok.to]?.let { list ->
                        (if (sortAlternatives) list.sorted() else list).joinToString("|")
                    }
                },
                sentences = sentences
            )
            mergeSpans(alreadyMultiToken + collapsed, sortAlternatives)
        } else {
            val cleaned = rawSpans.mapNotNull { span ->
                val cleanTags = span.tags.mapNotNull { parseTag(it)?.label }.distinct()
                if (cleanTags.isEmpty()) null else NamedEntitySpan(span.from, span.to, cleanTags)
            }
            mergeSpans(cleaned, sortAlternatives)
        }

        if (normalizedSpans.isEmpty()) return emptyList()

        data class Candidate(
            val from: Int,
            val to: Int,
            val tokenFrom: Int,
            val tokenTo: Int,
            val tag: String,
            val order: Int
        )

        val candidates = mutableListOf<Candidate>()
        val seen = HashSet<Triple<Int, Int, String>>()
        var order = 0

        for (span in normalizedSpans) {
            val (tokenFrom, tokenTo) = resolveTokenRange(span.from, span.to, tokens) ?: continue
            for (tag in span.tags) {
                if (seen.add(Triple(tokenFrom, tokenTo, tag))) {
                    candidates.add(Candidate(span.from, span.to, tokenFrom, tokenTo, tag, order++))
                }
            }
        }

        if (candidates.isEmpty()) return emptyList()

        // Sort outer spans before strictly nested spans to compute nesting depth in linear time
        val byInterval = candidates.sortedWith(
            compareBy<Candidate> { it.tokenFrom }
                .thenByDescending { it.tokenTo }
                .thenBy { it.order }
        )

        data class ActiveSpan(val tokenFrom: Int, val tokenTo: Int, val depth: Int)

        val depths = IntArray(byInterval.size)
        val active = ArrayList<ActiveSpan>()
        var monotonic = true

        for (i in byInterval.indices) {
            val cur = byInterval[i]
            if (monotonic) {
                while (active.isNotEmpty() && active.last().tokenTo <= cur.tokenFrom) {
                    active.removeAt(active.lastIndex)
                }
                if (active.isEmpty()) {
                    depths[i] = 0
                    active.add(ActiveSpan(cur.tokenFrom, cur.tokenTo, 0))
                } else {
                    val top = active.last()
                    when {
                        top.tokenFrom == cur.tokenFrom && top.tokenTo == cur.tokenTo -> {
                            // Same-interval sibling: identical depth, no need to push duplicate
                            depths[i] = top.depth
                        }
                        top.tokenTo >= cur.tokenTo -> {
                            // Strictly contained in top (which is the deepest ancestor in a monotonic stack)
                            val d = top.depth + 1
                            depths[i] = d
                            active.add(ActiveSpan(cur.tokenFrom, cur.tokenTo, d))
                        }
                        else -> {
                            // Crossing span (e.g. from multi-model disagreement): switch to active-set pruning
                            monotonic = false
                        }
                    }
                }
            }
            if (!monotonic) {
                active.removeAll { it.tokenTo <= cur.tokenFrom }
                if (active.isEmpty()) {
                    monotonic = true
                    depths[i] = 0
                    active.add(ActiveSpan(cur.tokenFrom, cur.tokenTo, 0))
                } else {
                    var maxParentDepth = -1
                    var duplicate = false
                    for (prev in active) {
                        if (prev.tokenFrom == cur.tokenFrom && prev.tokenTo == cur.tokenTo) {
                            duplicate = true
                        } else if (prev.tokenFrom <= cur.tokenFrom && prev.tokenTo >= cur.tokenTo) {
                            if (prev.depth > maxParentDepth) maxParentDepth = prev.depth
                        }
                    }
                    val d = maxParentDepth + 1
                    depths[i] = d
                    if (!duplicate) {
                        active.add(ActiveSpan(cur.tokenFrom, cur.tokenTo, d))
                    }
                }
            }
        }

        return byInterval.mapIndexed { idx, c ->
            ResolvedNamedEntitySpan(
                from = c.from,
                to = c.to,
                tokenFrom = c.tokenFrom,
                tokenTo = c.tokenTo,
                tag = c.tag,
                depth = depths[idx]
            )
        }
    }

    /**
     * Merge [NamedEntitySpan]s sharing identical `(from, to)` offsets, deduplicating tags.
     * When [sortTags] is true (e.g. combining multiple NE model files of one foundry), tags are
     * sorted alphabetically so the output does not depend on file order.
     */
    fun mergeSpans(spans: List<NamedEntitySpan>, sortTags: Boolean = false): List<NamedEntitySpan> {
        if (spans.isEmpty()) return emptyList()
        val grouped = LinkedHashMap<Pair<Int, Int>, MutableList<String>>()
        for (span in spans) {
            val list = grouped.getOrPut(span.from to span.to) { mutableListOf() }
            for (tag in span.tags) {
                if (tag.isNotEmpty() && tag != "_" && tag != "O" && tag !in list) {
                    list.add(tag)
                }
            }
        }
        return grouped.entries
            .sortedWith(compareBy<Map.Entry<Pair<Int, Int>, MutableList<String>>> { it.key.first }.thenByDescending { it.key.second })
            .mapNotNull { (range, tags) ->
                if (tags.isEmpty()) null
                else NamedEntitySpan(range.first, range.second, if (sortTags) tags.sorted() else tags)
            }
    }

    /**
     * Resolve character offsets [from]..[to] to an exclusive token index range `[tokenFrom, tokenTo)`.
     * Returns null if no token in [tokens] falls inside `[from, to]`.
     */
    private fun resolveTokenRange(from: Int, to: Int, tokens: List<KorapXmlTool.Span>): Pair<Int, Int>? {
        if (tokens.isEmpty() || from >= to) return null

        // First token with token.from >= from
        var low = 0
        var high = tokens.size
        while (low < high) {
            val mid = (low + high) ushr 1
            if (tokens[mid].from < from) low = mid + 1 else high = mid
        }
        val tokenFrom = low
        if (tokenFrom >= tokens.size || tokens[tokenFrom].from >= to) return null

        // Last token with token.to <= to
        low = 0
        high = tokens.size
        while (low < high) {
            val mid = (low + high) ushr 1
            if (tokens[mid].to <= to) low = mid + 1 else high = mid
        }
        val lastTokenIndex = low - 1
        if (lastTokenIndex < tokenFrom || tokens[lastTokenIndex].to <= from) return null

        return tokenFrom to (lastTokenIndex + 1)
    }
}
