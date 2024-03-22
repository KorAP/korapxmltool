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
        morphoMap: MutableMap<String, KorapXml2Conllu.MorphoSpan>?
    )
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

