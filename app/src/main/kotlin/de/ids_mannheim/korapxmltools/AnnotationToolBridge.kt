package de.ids_mannheim.korapxmltools

import java.util.logging.Logger

interface AnnotationToolBridge {
    val foundry: String
    val model: String
    val logger: Logger

    @Throws(java.lang.ArrayIndexOutOfBoundsException::class, java.lang.Exception::class)
    fun tagSentence(
        sentenceTokens: MutableList<String>,
        sentenceTokenOffsets: MutableList<String>,
        morphoMap: MutableMap<String, KorapXmlTool.MorphoSpan>?
    )
}


class AnnotationToolBridgeFactory {
    companion object {
        const val taggerFoundries = "marmot|opennlp|corenlp"
        const val parserFoundries = "malt|corenlp"

        fun getAnnotationToolBridge(foundry: String, model: String, LOGGER: Logger): AnnotationToolBridge? {
            when (foundry) {
                "marmot" -> return MarmotBridge(model, LOGGER)
                "opennlp" -> return OpenNlpBridge(model, LOGGER)
                "malt" -> return MaltParserBridge(model, LOGGER)
                "corenlp" -> return CoreNLPBridge(model, LOGGER)
                else -> LOGGER.severe("Unknown tagger/parser $foundry")
            }
            return null
        }

        // Get a tagger specifically
        fun getTagger(foundry: String, model: String, LOGGER: Logger): TaggerToolBridge? {
            when (foundry) {
                "marmot" -> return MarmotBridge(model, LOGGER)
                "opennlp" -> return OpenNlpBridge(model, LOGGER)
                "corenlp" -> return CoreNLPTaggerBridge(model, LOGGER)
                else -> LOGGER.severe("Unknown tagger $foundry")
            }
            return null
        }

        // Get a parser specifically (dependency or constituency)
        fun getParser(foundry: String, model: String, LOGGER: Logger, taggerModel: String? = null): AnnotationToolBridge? {
            when (foundry) {
                "malt" -> return MaltParserBridge(model, LOGGER)
                "corenlp" -> return CoreNLPBridge(model, LOGGER, taggerModel)
                else -> LOGGER.severe("Unknown parser $foundry")
            }
            return null
        }
    }
}

