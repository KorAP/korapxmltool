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
        const val taggerFoundries = "marmot|opennlp"
        const val parserFoundries = "malt"

        fun getAnnotationToolBridge(foundry: String, model: String, LOGGER: Logger): AnnotationToolBridge? {
            when (foundry) {
                "marmot" -> return MarmotBridge(model, LOGGER)
                "opennlp" -> return OpenNlpBridge(model, LOGGER)
                "malt" -> return MaltParserBridge(model, LOGGER)
                else -> LOGGER.severe("Unknown tagger $foundry")
            }
            return null
        }
    }
}

