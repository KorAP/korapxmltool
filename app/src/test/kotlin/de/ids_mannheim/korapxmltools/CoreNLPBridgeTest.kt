package de.ids_mannheim.korapxmltools

import edu.stanford.nlp.ling.CoreAnnotations
import org.junit.Assume
import java.io.File
import java.util.logging.Logger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class CoreNLPBridgeTest {

    @Test
    fun tokenOrderPosUpdatesFollowProvidedTokenization() {
        val tokens = listOf(
            KorapXmlTool.Span(0, 3),
            KorapXmlTool.Span(4, 18),
            KorapXmlTool.Span(19, 22)
        )
        val morpho = linkedMapOf(
            "0-3" to KorapXmlTool.MorphoSpan(lemma = "die", xpos = "OLD_ART"),
            "4-18" to KorapXmlTool.MorphoSpan(lemma = "Schüler:innen", xpos = "OLD_NN"),
            "19-22" to KorapXmlTool.MorphoSpan(lemma = "das", xpos = "OLD_PDS")
        )

        CoreNLPBridge.mergeMorphoPosAssignmentsByTokenOrder(
            posTags = listOf("ART", "NN", "PDS"),
            tokens = tokens,
            morpho = morpho
        )

        assertEquals("ART", morpho["0-3"]?.xpos)
        assertEquals("NN", morpho["4-18"]?.xpos)
        assertEquals("PDS", morpho["19-22"]?.xpos)
    }

    @Test
    fun retokenizedLeafDoesNotShiftPosUpdatesOntoFollowingToken() {
        val assignments = listOf(
            CoreNLPBridge.PosAssignment(0, 3, "ART"),
            CoreNLPBridge.PosAssignment(4, 12, "NN"),
            CoreNLPBridge.PosAssignment(12, 13, "$."),
            CoreNLPBridge.PosAssignment(13, 18, "TRUNC"),
            CoreNLPBridge.PosAssignment(19, 22, "PDS")
        )
        val tokens = listOf(
            KorapXmlTool.Span(0, 3),
            KorapXmlTool.Span(4, 18),
            KorapXmlTool.Span(19, 22)
        )
        val morpho = linkedMapOf(
            "0-3" to KorapXmlTool.MorphoSpan(lemma = "die", xpos = "OLD_ART"),
            "4-18" to KorapXmlTool.MorphoSpan(lemma = "Schüler:innen", xpos = "OLD_NN"),
            "19-22" to KorapXmlTool.MorphoSpan(lemma = "das", xpos = "OLD_PDS")
        )

        CoreNLPBridge.mergeMorphoPosAssignmentsByExactOffsets(assignments, tokens, morpho, Logger.getAnonymousLogger())

        assertEquals("ART", morpho["0-3"]?.xpos)
        assertEquals("OLD_NN", morpho["4-18"]?.xpos, "Split CoreNLP leaves must not overwrite the original surface token")
        assertEquals("PDS", morpho["19-22"]?.xpos, "The following token must keep its own exact POS update instead of inheriting punctuation")
        assertFalse(morpho.containsKey("4-12"))
        assertFalse(morpho.containsKey("12-13"))
        assertFalse(morpho.containsKey("13-18"))
    }

    @Test
    fun exactLeafMatchCanCreateMissingMorphoEntry() {
        val morpho = mutableMapOf<String, KorapXmlTool.MorphoSpan>()
        CoreNLPBridge.mergeMorphoPosAssignmentsByExactOffsets(
            assignments = listOf(CoreNLPBridge.PosAssignment(10, 15, "VVFIN")),
            tokens = listOf(KorapXmlTool.Span(10, 15)),
            morpho = morpho,
            logger = Logger.getAnonymousLogger()
        )

        val created = morpho["10-15"]
        assertNotNull(created)
        assertEquals("VVFIN", created.xpos)
    }

    // Tokens containing characters at which CoreNLP's whitespace tokenizer splits (U+2009, U+000B)
    // or a whitespace-only token it drops (U+3000) used to shift all following POS tags.
    private val trickyTokens = listOf("Er", "sagt", "z.\u2009B.", "\u3000", "der", "Hund", "bellt", "a\u000Bb", ".")

    private fun trickySentence(): Triple<NonBmpString, Array<KorapXmlTool.Span>, Array<KorapXmlTool.Span>> {
        val text = trickyTokens.joinToString(" ")
        var offset = 0
        val spans = trickyTokens.map { token ->
            KorapXmlTool.Span(offset, offset + token.length).also { offset += token.length + 1 }
        }.toTypedArray()
        return Triple(NonBmpString(text), spans, arrayOf(KorapXmlTool.Span(0, text.length)))
    }

    private fun modelFile(name: String): File {
        val model = listOf(File("libs/$name"), File("../libs/$name")).firstOrNull { it.exists() }
        Assume.assumeTrue("CoreNLP model $name not found", model != null)
        return model!!
    }

    @Test
    fun sentenceAnnotationKeepsOneCoreLabelPerBaseToken() {
        val (text, spans, _) = trickySentence()
        val annotation = CoreNLPBridge.buildSentenceAnnotation(spans.toList(), text)

        val labels = annotation.get(CoreAnnotations.TokensAnnotation::class.java)
        assertEquals(trickyTokens, labels.map { it.word() })
        assertEquals(spans.map { it.from }, labels.map { it.beginPosition() })
        assertEquals(spans.map { it.to }, labels.map { it.endPosition() })
        assertEquals(1, annotation.get(CoreAnnotations.SentencesAnnotation::class.java).size)
    }

    @Test
    fun taggerDoesNotShiftPosTagsAfterTokensWithInnerWhitespace() {
        val tagger = CoreNLPTaggerBridge(modelFile("german-fast.tagger").path, Logger.getAnonymousLogger())
        val (text, spans, sentences) = trickySentence()

        val morpho = tagger.tagText(spans, sentences, text)

        spans.forEach { assertNotNull(morpho["${it.from}-${it.to}"], "Every base token must be tagged") }
        assertEquals("NN", morpho["${spans[5].from}-${spans[5].to}"]?.xpos, "Hund")
        assertEquals("VVFIN", morpho["${spans[6].from}-${spans[6].to}"]?.xpos, "bellt")
        assertEquals("\$.", morpho["${spans[8].from}-${spans[8].to}"]?.xpos, "final punctuation")
    }

    @Test
    fun parserKeepsTreeAndPosAlignedForTokensWithInnerWhitespace() {
        val tagger = modelFile("german-fast.tagger")
        val parser = CoreNLPBridge(modelFile("germanSR.ser.gz").path, Logger.getAnonymousLogger(), tagger.path)
        val (text, spans, sentences) = trickySentence()
        val morpho = mutableMapOf<String, KorapXmlTool.MorphoSpan>()

        val trees = parser.parseConstituency(spans, morpho, sentences, text)

        assertEquals(1, trees.size, "The sentence must not be skipped because of a token count mismatch")
        val root = trees.single().nodes.first()
        assertEquals(spans.first().from, root.from)
        assertEquals(spans.last().to, root.to)
        assertTrue(spans.all { morpho.containsKey("${it.from}-${it.to}") }, "Every base token must get a POS tag")
        assertEquals("NN", morpho["${spans[5].from}-${spans[5].to}"]?.xpos, "Hund")
        assertEquals("\$.", morpho["${spans[8].from}-${spans[8].to}"]?.xpos, "final punctuation")
    }
}
