package de.ids_mannheim.korapxmltools

import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.Word
import java.io.File
import java.util.logging.Logger

/**
 * Named-entity recognition with a CoreNLP CRF model (e.g. the German
 * german.conll.germeval2014.hgc_175m_600.crf.ser.gz). Fills only the ne field
 * of the returned spans; untagged tokens ("O") get no span.
 *
 * The model's own tags (PERSON, LOCATION, ORGANIZATION, MISC) are kept; with
 * [mapTags], they are mapped to the CoNLL tag set of older CoreNLP models and
 * Kalamar's annotation assistant (PERSON -> I-PER, ...).
 */
class CoreNLPNerBridge(
    override val model: String,
    override val logger: Logger,
    private val mapTags: Boolean = false
) : TaggerToolBridge() {
    override val foundry = "corenlp"

    private val classifier: CRFClassifier<CoreLabel>

    init {
        logger.info("Initializing CoreNLP NER with model $model")

        if (!File(model).exists()) {
            throw IllegalArgumentException("Model file not found: $model")
        }

        classifier = CRFClassifier.getClassifier(model)
        logger.info("CoreNLP NER initialized successfully")
    }

    override fun tagSentence(
        sentenceTokens: MutableList<String>,
        sentenceTokenOffsets: MutableList<String>,
        morphoMap: MutableMap<String, KorapXmlTool.MorphoSpan>?
    ) {
        if (sentenceTokens.isEmpty()) return

        // Classify the base tokens directly (no CoreNLP tokenization), so labels stay aligned.
        // classifySentence, unlike classify, first adds the features the model was trained
        // with (e.g. word shapes); without them recall drops by more than half.
        val classified = classifier.classifySentence(sentenceTokens.map { Word(it) })

        if (classified.size != sentenceTokens.size) {
            logger.warning(
                "CoreNLP NER token count mismatch: ner=${classified.size}, input=${sentenceTokens.size}; " +
                    "skipping sentence to avoid misalignment"
            )
            return
        }

        classified.forEachIndexed { idx, label ->
            val tag = label.get(CoreAnnotations.AnswerAnnotation::class.java)
            if (tag.isNullOrEmpty() || tag == classifier.backgroundSymbol()) return@forEachIndexed
            morphoMap?.set(sentenceTokenOffsets[idx], KorapXmlTool.MorphoSpan(ne = if (mapTags) mapTag(tag) else tag))
        }
    }

    companion object {
        private val TAG_MAPPING = mapOf(
            "PERSON" to "I-PER",
            "LOCATION" to "I-LOC",
            "ORGANIZATION" to "I-ORG",
            "MISC" to "I-MISC"
        )

        fun mapTag(tag: String): String = TAG_MAPPING[tag] ?: tag
    }
}
