package de.ids_mannheim.korapxmltools

import opennlp.tools.postag.POSModel
import opennlp.tools.postag.POSTaggerME
import java.io.File
import java.util.*
import java.util.logging.Logger


class OpenNlpBridge(override val model: String, override val logger: Logger) : TaggerToolBridge() {

    override val foundry = "opennlp"
    val tagger: POSTaggerME

    companion object {
        var POSmodel : POSModel? = null
    }

    init {

        synchronized(model) {
            if (POSmodel == null) {
                logger.info("Initializing OpenNLP with model $model")
                POSmodel = POSModel(File(model as String).inputStream())
                logger.info("Model $model loaded")
            }
        }

        tagger = POSTaggerME(POSmodel)

    }

    override fun tagSentence(
        sentenceTokens: MutableList<String>,
        sentenceTokenOffsets: MutableList<String>,
        morphoMap: MutableMap<String, KorapXml2Conllu.MorphoSpan>?
    ) {

        // Perform POS tagging
        val result = tagger.tag(sentenceTokens.toTypedArray())
        val probs = tagger.probs()
        for (i in 0 until result.size) {
            val taggedWord = KorapXml2Conllu.MorphoSpan(
                xpos = result[i],
                misc = String.format(locale = Locale.ROOT, "%.5f", probs[i])
            )
            morphoMap?.set(sentenceTokenOffsets[i], taggedWord)
        }
    }

}