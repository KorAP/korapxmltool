package de.ids_mannheim.korapxmltools

import marmot.morph.MorphTagger
import marmot.morph.Sentence
import marmot.morph.Word
import marmot.util.FileUtils
import java.util.logging.Logger

class MarmotBridge(override val model: String, override val logger: Logger) : TaggerToolBridge() {
    override val foundry = "marmot"
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