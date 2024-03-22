package de.ids_mannheim.korapxmltools

import marmot.morph.MorphTagger
import marmot.morph.Sentence
import marmot.morph.Word
import marmot.util.FileUtils
import org.maltparser.MaltParserService
import org.maltparser.core.exception.MaltChainedException
import org.maltparser.core.syntaxgraph.DependencyStructure
import java.util.logging.Logger

interface AnnotationToolBridge {
    val model: String
    val logger: Logger

    @Throws(java.lang.ArrayIndexOutOfBoundsException::class, java.lang.Exception::class)
    fun tagSentence(
        sentenceTokens: MutableList<String>,
        sentenceTokenOffsets: MutableList<String>,
        morphoMap: MutableMap<String, KorapXml2Conllu.MorphoSpan>?
    )
}

abstract class TaggerToolBridge : AnnotationToolBridge {

    fun tagText(
        tokens: Array<KorapXml2Conllu.Span>, sentenceSpans: Array<KorapXml2Conllu.Span>?, text: String
    ): MutableMap<String, KorapXml2Conllu.MorphoSpan> {
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

abstract class ParserToolBridge : AnnotationToolBridge {
    fun parseText(
        tokens: Array<KorapXml2Conllu.Span>,
        morpho: MutableMap<String, KorapXml2Conllu.MorphoSpan>?,
        sentenceSpans: Array<KorapXml2Conllu.Span>?,
        text: String
    ): MutableMap<String, KorapXml2Conllu.MorphoSpan> {
        val sentence_tokens = mutableListOf<String>()
        val sentence_token_offsets = mutableListOf<String>()
        var token_index = 1
        var sentence_index = 0
        tokens.forEach { span ->
            if (span.from >= (sentenceSpans?.get(sentence_index)?.to ?: 11111110)) {
                tagSentence(sentence_tokens, sentence_token_offsets, morpho)
                sentence_tokens.clear()
                sentence_token_offsets.clear()
                sentence_index++
                token_index = 1

            }
            sentence_tokens.add(
                "$token_index\t${
                    text.substring(
                        span.from, span.to
                    )
                }\t_\t${morpho?.get("${span.from}-${span.to}")?.xpos ?: "_"}\t${morpho?.get("${span.from}-${span.to}")?.xpos ?: "_"}\t${
                    morpho?.get(
                        "${span.from}-${span.to}"
                    )?.feats ?: "_"
                }\t_\t_\t_\t_"
            )
            sentence_token_offsets.add("${span.from}-${span.to}")
            token_index++
        }
        if (sentence_tokens.size > 0) {
            try {
                tagSentence(sentence_tokens, sentence_token_offsets, morpho)
            } catch (e: ArrayIndexOutOfBoundsException) {
                logger.warning("Tagging failed: ${e.message} ${e.stackTrace} ${sentence_tokens.joinToString { " " }}")
            }
        }
        return morpho!!
    }
}


class AnnotationToolBridgeFactory {
    companion object {
        const val taggerFoundries = "marmot"
        const val parserFoundries = "malt"

        fun getAnnotationToolBridge(foundry: String, model: String, LOGGER: Logger): AnnotationToolBridge? {
            when (foundry) {
                "marmot" -> return MarmotBridge(model, LOGGER)
                "malt" -> return MaltParserBridge(model, LOGGER)
                else -> LOGGER.severe("Unknown tagger $foundry")
            }
            return null
        }
    }
}

class MaltParserBridge(override val model: String, override val logger: Logger) : ParserToolBridge() {
    companion object {
        fun getFoundry(): String {
            return "malt"
        }
    }

    val tagger: MaltParserService

    init {
        logger.info("Initializing MaltParser with model $model")
        tagger = MaltParserService()
        if (model.contains("/")) {
            val dirName = model.substringBeforeLast("/")
            val modelName = model.substringAfterLast("/")
            logger.info("Loading model $modelName from $dirName")
            tagger.initializeParserModel("-w $dirName -c $modelName -m parse")
        } else {
            tagger.initializeParserModel("-c $model -m parse")
        }
        logger.info("Model $model loaded")
    }


    @Throws(MaltChainedException::class)
    override fun tagSentence(
        sentenceTokens: MutableList<String>,
        sentenceTokenOffsets: MutableList<String>,
        morpho: MutableMap<String, KorapXml2Conllu.MorphoSpan>?
    ) {
        val result = tagger.parse(sentenceTokens.toTypedArray())

        (result as DependencyStructure).edges.forEach { edge ->
            val from = edge.source.index
            val head = edge.target.index
            val label = edge.toString()
            if (label.contains("DEPREL:")) {
                val rel = edge.toString().substringAfter("DEPREL:")
                val old = morpho?.get(sentenceTokenOffsets[head - 1])
                morpho?.set(
                    sentenceTokenOffsets[head - 1], KorapXml2Conllu.MorphoSpan(
                        lemma = old?.lemma, xpos = old?.xpos, feats = old?.feats, head = from.toString(), deprel = rel
                    )
                )
            }
        }
    }
}

class MarmotBridge(override val model: String, override val logger: Logger) : TaggerToolBridge() {
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
                xpos = result[i][0].split("|")[0], feats = result[i][1]
            )
            morphoMap?.set(sentenceTokenOffsets[i], taggedWord)
        }
    }

}