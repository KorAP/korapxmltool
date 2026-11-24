package de.ids_mannheim.korapxmltools

import java.util.logging.Logger
import kotlin.test.Test
import kotlin.test.assertEquals

class ToolBridgeSentenceTest {
    private val text = NonBmpString("a b c d e")
    private val tokens = Array(5) { KorapXmlTool.Span(2 * it, 2 * it + 1) }
    // The last sentence ends before the last two tokens
    private val sentences = arrayOf(KorapXmlTool.Span(0, 3), KorapXmlTool.Span(4, 5))
    private val seen = mutableListOf<List<String>>()

    @Test
    fun taggerKeepsTokensAfterTheLastSentence() {
        val tagger = object : TaggerToolBridge() {
            override val foundry = "test"
            override val model = ""
            override val logger = Logger.getAnonymousLogger()
            override fun tagSentence(
                sentenceTokens: MutableList<String>,
                sentenceTokenOffsets: MutableList<String>,
                morphoMap: MutableMap<String, KorapXmlTool.MorphoSpan>?
            ) {
                seen.add(sentenceTokens.toList())
            }
        }
        tagger.tagText(tokens, sentences, text)
        assertEquals(listOf(listOf("a", "b"), listOf("c"), listOf("d", "e")), seen)
    }

    @Test
    fun parserKeepsTokensAfterTheLastSentence() {
        val parser = object : ParserToolBridge() {
            override val foundry = "test"
            override val model = ""
            override val logger = Logger.getAnonymousLogger()
            override fun tagSentence(
                sentenceTokens: MutableList<String>,
                sentenceTokenOffsets: MutableList<String>,
                morphoMap: MutableMap<String, KorapXmlTool.MorphoSpan>?
            ) {
                seen.add(sentenceTokens.map { it.split("\t")[1] })
            }
        }
        parser.parseText(tokens, mutableMapOf(), sentences, text)
        assertEquals(listOf(listOf("a", "b"), listOf("c"), listOf("d", "e")), seen)
    }
}
