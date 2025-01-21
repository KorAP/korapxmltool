package de.ids_mannheim.korapxmltools

import org.maltparser.MaltParserService
import org.maltparser.core.exception.MaltChainedException
import org.maltparser.core.syntaxgraph.DependencyStructure
import java.util.logging.Logger

class MaltParserBridge(override val model: String, override val logger: Logger) : ParserToolBridge() {
    override val foundry = "malt"

    val tagger: MaltParserService

    init {
        logger.info("Initializing MaltParser with model $model")
        synchronized(MaltParserService::class.java) {
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
    }

    @Throws(MaltChainedException::class)
    override fun tagSentence(
        sentenceTokens: MutableList<String>,
        sentenceTokenOffsets: MutableList<String>,
        morpho: MutableMap<String, KorapXmlTool.MorphoSpan>?
    ) {
        val result = tagger.parse(sentenceTokens.toTypedArray())

        (result as DependencyStructure).edges.forEach { edge ->
            val from = edge.source.index
            val head = edge.target.index
            val label = edge.toString()
            if (label.contains("DEPREL:")) {
                val rel = edge.toString().substringAfter("DEPREL:").trim()
                val old = morpho?.get(sentenceTokenOffsets[head - 1])
                morpho?.set(
                    sentenceTokenOffsets[head - 1], KorapXmlTool.MorphoSpan(
                        lemma = old?.lemma, xpos = old?.xpos, feats = old?.feats, head = from.toString(), deprel = rel
                    )
                )
            }
        }
    }
}