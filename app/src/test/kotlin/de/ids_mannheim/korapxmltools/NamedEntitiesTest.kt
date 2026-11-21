package de.ids_mannheim.korapxmltools

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class NamedEntitiesTest {

    private fun spansOf(words: List<String>): List<KorapXmlTool.Span> {
        var offset = 0
        return words.map { w ->
            KorapXmlTool.Span(offset, offset + w.length).also { offset += w.length + 1 }
        }
    }

    @Test
    fun normalizesVerboseCoreNlpAndBioTagsAndDropsOutside() {
        assertEquals("PER", NamedEntities.parseTag("PERSON")?.label)
        assertEquals("LOC", NamedEntities.parseTag("LOCATION")?.label)
        assertEquals("ORG", NamedEntities.parseTag("ORGANIZATION")?.label)
        assertEquals("MISC", NamedEntities.parseTag("MISC")?.label)
        assertEquals("PER", NamedEntities.parseTag("B-PER")?.label)
        assertEquals("PER", NamedEntities.parseTag("I-PER")?.label)
        assertEquals("LOC", NamedEntities.parseTag("B-LOCATION")?.label)
        assertEquals("MISC", NamedEntities.parseTag("B-OTH")?.label)
        assertEquals("PER:part", NamedEntities.parseTag("B-PERpart")?.label)
        assertEquals("PER:deriv", NamedEntities.parseTag("I-PERderiv")?.label)
        assertEquals("LOC:part", NamedEntities.parseTag("LOCpart")?.label)
        assertEquals("LOC:deriv", NamedEntities.parseTag("B-LOCderiv")?.label)
        assertEquals("ORG:part", NamedEntities.parseTag("B-ORGpart")?.label)
        assertEquals("ORG:deriv", NamedEntities.parseTag("ORGderiv")?.label)
        assertEquals("MISC:part", NamedEntities.parseTag("B-OTHpart")?.label)
        assertEquals("MISC:deriv", NamedEntities.parseTag("I-OTHderiv")?.label)
        assertEquals("MISC:part", NamedEntities.parseTag("OTH:part")?.label)
        assertEquals("persName:surname", NamedEntities.parseTag("persName:surname")?.label)
        assertNull(NamedEntities.parseTag("O"))
        assertNull(NamedEntities.parseTag("_"))
        assertNull(NamedEntities.parseTag(""))
    }

    @Test
    fun collapsesGermEvalPartAndDerivWithSuffixSpecifiers() {
        val tokens = spansOf(listOf("Goethe-Institut", "und", "deutsche", "Kultur"))
        val tags = listOf("B-ORG|B-PERpart", "O", "B-LOCderiv", "O")
        val tagMap = tokens.zip(tags).associate { (t, tag) -> "${t.from}-${t.to}" to tag }

        val collapsed = NamedEntities.collapseTokenTagsToSpans(tokens, { tagMap["${it.from}-${it.to}"] })
        assertEquals(
            listOf(
                NamedEntitySpan(tokens[0].from, tokens[0].to, listOf("ORG", "PER:part")),
                NamedEntitySpan(tokens[2].from, tokens[2].to, listOf("LOC:deriv"))
            ),
            collapsed
        )
    }

    @Test
    fun collapsesMultiTokenBareAndIoEntitiesRespectingGapsAndSentences() {
        val tokens = spansOf(listOf("Konrad", "Alberti", "und", "Maria", "Reis", "Berlin", "Hamburg"))
        val tags = listOf("PERSON", "PERSON", "O", "PERSON", "PERSON", "LOCATION", "LOCATION")
        val sentences = arrayOf(
            KorapXmlTool.Span(tokens[0].from, tokens[5].to),
            KorapXmlTool.Span(tokens[6].from, tokens[6].to)
        )
        val tagMap = tokens.zip(tags).associate { (t, tag) -> "${t.from}-${t.to}" to tag }

        val collapsed = NamedEntities.collapseTokenTagsToSpans(tokens, { tagMap["${it.from}-${it.to}"] }, sentences)

        assertEquals(
            listOf(
                NamedEntitySpan(tokens[0].from, tokens[1].to, listOf("PER")),
                NamedEntitySpan(tokens[3].from, tokens[4].to, listOf("PER")),
                NamedEntitySpan(tokens[5].from, tokens[5].to, listOf("LOC")),
                NamedEntitySpan(tokens[6].from, tokens[6].to, listOf("LOC"))
            ),
            collapsed
        )
    }

    @Test
    fun bioSplitsAdjacentSameClassEntities() {
        val tokens = spansOf(listOf("Anna", "Müller", "Peter", "Schmidt"))
        val tags = listOf("B-PER", "I-PER", "B-PER", "I-PER")
        val tagMap = tokens.zip(tags).associate { (t, tag) -> "${t.from}-${t.to}" to tag }

        val collapsed = NamedEntities.collapseTokenTagsToSpans(tokens, { tagMap["${it.from}-${it.to}"] })

        assertEquals(
            listOf(
                NamedEntitySpan(tokens[0].from, tokens[1].to, listOf("PER")),
                NamedEntitySpan(tokens[2].from, tokens[3].to, listOf("PER"))
            ),
            collapsed
        )
    }

    @Test
    fun nestedEntitiesGetDistinctSpansAndNestingDepth() {
        val tokens = spansOf(listOf("FC", "Kickers", "Darmstadt"))
        val tags = listOf("B-ORG", "I-ORG", "I-ORG|B-LOC")
        val tagMap = tokens.zip(tags).associate { (t, tag) -> "${t.from}-${t.to}" to tag }

        val collapsed = NamedEntities.collapseTokenTagsToSpans(tokens, { tagMap["${it.from}-${it.to}"] })
        assertEquals(
            listOf(
                NamedEntitySpan(tokens[0].from, tokens[2].to, listOf("ORG")),
                NamedEntitySpan(tokens[2].from, tokens[2].to, listOf("LOC"))
            ),
            collapsed
        )

        val resolved = NamedEntities.resolveForKrill(collapsed, tokens)
        assertEquals(
            listOf(
                ResolvedNamedEntitySpan(tokens[0].from, tokens[2].to, 0, 3, "ORG", depth = 0),
                ResolvedNamedEntitySpan(tokens[2].from, tokens[2].to, 2, 3, "LOC", depth = 1)
            ),
            resolved
        )
    }

    @Test
    fun legacyPerTokenBioSpansCollapseWhenResolvedForKrill() {
        val tokens = spansOf(listOf("New", "York", "is", "huge"))
        val raw = listOf(
            NamedEntitySpan(tokens[0].from, tokens[0].to, listOf("B-LOC")),
            NamedEntitySpan(tokens[1].from, tokens[1].to, listOf("I-LOC"))
        )

        val resolved = NamedEntities.resolveForKrill(raw, tokens)
        assertEquals(
            listOf(ResolvedNamedEntitySpan(tokens[0].from, tokens[1].to, 0, 2, "LOC", depth = 0)),
            resolved
        )
    }

    @Test
    fun nestedSameClassEntitiesKeepBothOuterAndInnerSpans() {
        val tokens = spansOf(listOf("Verein", "FC", "Darmstadt"))
        val tags = listOf("B-ORG", "I-ORG|B-ORG", "I-ORG|I-ORG")
        val tagMap = tokens.zip(tags).associate { (t, tag) -> "${t.from}-${t.to}" to tag }

        val collapsed = NamedEntities.collapseTokenTagsToSpans(tokens, { tagMap["${it.from}-${it.to}"] })
        assertEquals(
            listOf(
                NamedEntitySpan(tokens[0].from, tokens[2].to, listOf("ORG")),
                NamedEntitySpan(tokens[1].from, tokens[2].to, listOf("ORG"))
            ),
            collapsed
        )

        val resolved = NamedEntities.resolveForKrill(collapsed, tokens)
        assertEquals(
            listOf(
                ResolvedNamedEntitySpan(tokens[0].from, tokens[2].to, 0, 3, "ORG", depth = 0),
                ResolvedNamedEntitySpan(tokens[1].from, tokens[2].to, 1, 3, "ORG", depth = 1)
            ),
            resolved
        )
    }

    @Test
    fun nestedSameClassInnerEntityClosesWhileOuterContinues() {
        val tokens = spansOf(listOf("A", "B", "C", "D"))
        val tags = listOf("B-ORG", "I-ORG|B-ORG", "I-ORG|I-ORG", "I-ORG")
        val tagMap = tokens.zip(tags).associate { (t, tag) -> "${t.from}-${t.to}" to tag }

        val collapsed = NamedEntities.collapseTokenTagsToSpans(tokens, { tagMap["${it.from}-${it.to}"] })
        assertEquals(
            listOf(
                NamedEntitySpan(tokens[0].from, tokens[3].to, listOf("ORG")),
                NamedEntitySpan(tokens[1].from, tokens[2].to, listOf("ORG"))
            ),
            collapsed
        )

        // Also test BILOU/BIOES with E- and S- on nested same-class entities
        val bioesTags = listOf("B-ORG", "I-ORG|B-ORG", "I-ORG|E-ORG", "E-ORG")
        val bioesMap = tokens.zip(bioesTags).associate { (t, tag) -> "${t.from}-${t.to}" to tag }
        assertEquals(collapsed, NamedEntities.collapseTokenTagsToSpans(tokens, { bioesMap["${it.from}-${it.to}"] }))

        val sameStartTags = listOf("B-ORG|B-ORG", "I-ORG|I-ORG", "I-ORG", "O")
        val sameStartMap = tokens.zip(sameStartTags).associate { (t, tag) -> "${t.from}-${t.to}" to tag }
        assertEquals(
            listOf(
                NamedEntitySpan(tokens[0].from, tokens[2].to, listOf("ORG")),
                NamedEntitySpan(tokens[0].from, tokens[1].to, listOf("ORG"))
            ),
            NamedEntities.collapseTokenTagsToSpans(tokens, { sameStartMap["${it.from}-${it.to}"] })
        )
    }

    @Test
    fun legacyUnnormalizedCoreNlpPerTokenSpansCollapseWhenResolvedForKrill() {
        // v4.2.x/v4.3.0 without --map-ne-tags wrote raw CoreNLP tags (PERSON, LOCATION, ORGANIZATION, MISC) per token
        val tokens = spansOf(listOf("Konrad", "Alberti", "und", "Deutsche", "Welle", "Berlin"))
        val raw = listOf(
            NamedEntitySpan(tokens[0].from, tokens[0].to, listOf("PERSON")),
            NamedEntitySpan(tokens[1].from, tokens[1].to, listOf("PERSON")),
            NamedEntitySpan(tokens[3].from, tokens[3].to, listOf("MISC")),
            NamedEntitySpan(tokens[4].from, tokens[4].to, listOf("MISC")),
            NamedEntitySpan(tokens[5].from, tokens[5].to, listOf("LOCATION"))
        )

        val resolved = NamedEntities.resolveForKrill(raw, tokens)
        assertEquals(
            listOf(
                ResolvedNamedEntitySpan(tokens[0].from, tokens[1].to, 0, 2, "PER", depth = 0),
                ResolvedNamedEntitySpan(tokens[3].from, tokens[4].to, 3, 5, "MISC", depth = 0),
                ResolvedNamedEntitySpan(tokens[5].from, tokens[5].to, 5, 6, "LOC", depth = 0)
            ),
            resolved
        )
    }

    @Test
    fun modernAdjacentSingleTokenBareSpansAreNotMergedByResolveForKrill() {
        val tokens = spansOf(listOf("Anna", "Peter"))
        val raw = listOf(
            NamedEntitySpan(tokens[0].from, tokens[0].to, listOf("PER")),
            NamedEntitySpan(tokens[1].from, tokens[1].to, listOf("PER"))
        )

        val resolved = NamedEntities.resolveForKrill(raw, tokens)
        assertEquals(
            listOf(
                ResolvedNamedEntitySpan(tokens[0].from, tokens[0].to, 0, 1, "PER", depth = 0),
                ResolvedNamedEntitySpan(tokens[1].from, tokens[1].to, 1, 2, "PER", depth = 0)
            ),
            resolved
        )
    }

    @Test
    fun nestingDepthHandlesSiblingsCrossingSpansAndDeepNesting() {
        val tokens = spansOf(listOf("t0", "t1", "t2", "t3", "t4", "t5", "t6"))
        val raw = listOf(
            NamedEntitySpan(tokens[0].from, tokens[3].to, listOf("ORG")),          // [0, 4) depth 0
            NamedEntitySpan(tokens[0].from, tokens[2].to, listOf("PER", "LOC")),   // [0, 3) siblings, both depth 1
            NamedEntitySpan(tokens[1].from, tokens[4].to, listOf("MISC")),         // [1, 5) crosses [0,4) and [0,3), depth 0
            NamedEntitySpan(tokens[2].from, tokens[2].to, listOf("PER:part")),     // [2, 3) inside [0,4), [0,3), [1,5) -> depth 2
            NamedEntitySpan(tokens[6].from, tokens[6].to, listOf("LOC"))           // [6, 7) after gap -> resets to depth 0
        )

        val resolved = NamedEntities.resolveForKrill(raw, tokens)
        assertEquals(
            listOf(
                ResolvedNamedEntitySpan(tokens[0].from, tokens[3].to, 0, 4, "ORG", depth = 0),
                ResolvedNamedEntitySpan(tokens[0].from, tokens[2].to, 0, 3, "PER", depth = 1),
                ResolvedNamedEntitySpan(tokens[0].from, tokens[2].to, 0, 3, "LOC", depth = 1),
                ResolvedNamedEntitySpan(tokens[1].from, tokens[4].to, 1, 5, "MISC", depth = 0),
                ResolvedNamedEntitySpan(tokens[2].from, tokens[2].to, 2, 3, "PER:part", depth = 2),
                ResolvedNamedEntitySpan(tokens[6].from, tokens[6].to, 6, 7, "LOC", depth = 0)
            ),
            resolved
        )
    }
}
