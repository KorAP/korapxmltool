package de.ids_mannheim.korapxmltools

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import marmot.morph.MorphTagger
import marmot.morph.Sentence
import marmot.morph.Word
import marmot.util.FileUtils
import java.util.logging.Logger
import kotlin.jvm.Throws

abstract class AnnotationToolBridge {
    abstract val model: String
    abstract val logger: Logger

    @Throws(java.lang.ArrayIndexOutOfBoundsException::class, java.lang.Exception::class)
    abstract fun tagSentence(sentenceTokens: MutableList<String>, sentenceTokenOffsets: MutableList<String>, morphoMap: MutableMap<String, KorapXml2Conllu.MorphoSpan>?)

    fun tagText(tokens: Array<KorapXml2Conllu.Span>, sentenceSpans: Array<KorapXml2Conllu.Span>?,  text: String): MutableMap<String, KorapXml2Conllu.MorphoSpan> {
        val coroutineScope = CoroutineScope(Dispatchers.Default)

        val sentence_tokens = mutableListOf<String>()
        val sentence_token_offsets = mutableListOf<String>()
        val morphoMap = mutableMapOf<String, KorapXml2Conllu.MorphoSpan>()
        var token_index = 0
        var sentence_index = 0
        tokens.forEach { span ->
            if (span.from >= (sentenceSpans?.get(sentence_index)?.to ?: 11111110)) {
                tagSentence(sentence_tokens, sentence_token_offsets, morphoMap)
                sentence_tokens.clear()
                sentence_token_offsets.clear()
                sentence_index++
                token_index = 1

            }
            sentence_tokens.add(text.substring(span.from, span.to))
            sentence_token_offsets.add("${span.from}-${span.to}")
            token_index++
        }
        if (sentence_tokens.size > 0) {
            try {
                tagSentence(sentence_tokens, sentence_token_offsets, morphoMap)
            } catch (e: ArrayIndexOutOfBoundsException) {
                logger.warning("Tagging failed: ${e.message} ${e.stackTrace} ${sentence_tokens.joinToString { " " }}")
            }
        }
        return morphoMap
    }
}


class AnnotationToolBridgeFactory {
    companion object {
        fun getAnnotationToolBridge(taggerName: String, taggerModel: String, LOGGER: Logger): AnnotationToolBridge? {
            if (taggerName == "marmot") {
                return MarmotBridge(taggerModel, LOGGER)
            } else {
                LOGGER.warning("Unknown tagger $taggerName")
                return null
            }
        }
    }
}

class MarmotBridge(override val model: String, override val logger: Logger) : AnnotationToolBridge() {

    val tagger: MorphTagger

    init {
        logger.info("Initializing MarMoT with model $model")
        tagger = FileUtils.loadFromFile(model)
        //tagger.setMaxLevel(100)
        logger.info("Model $model loaded")
    }

    @Throws(java.lang.ArrayIndexOutOfBoundsException::class, java.lang.Exception::class)
    override fun tagSentence(
        sentenceTokens: MutableList<String>,
        sentenceTokenOffsets: MutableList<String>,
        morphoMap: MutableMap<String, KorapXml2Conllu.MorphoSpan>?
    ) {
        val sentence = Sentence(sentenceTokens.map { Word(it) })
        var result: List<List<String>>
        result = tagger.tag(sentence)  // LOGGER.info("Marmot tagger finished")// return
        for (i in 0 until result.size) {
            val taggedWord = KorapXml2Conllu.MorphoSpan(
                xpos = result[i][0].split("|")[0],
                feats = result[i][1]
            )
            morphoMap?.set(sentenceTokenOffsets[i], taggedWord)
        }
    }

}