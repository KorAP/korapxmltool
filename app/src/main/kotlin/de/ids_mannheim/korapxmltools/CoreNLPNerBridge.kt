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
 * [mapTags], they are mapped to CoNLL BIO tags (PERSON -> B-PER for the first token
 * of an entity, I-PER for the following ones; the IOB2 scheme of GermEval 2014). The
 * models tag in the IO scheme, so two adjacent entities of the same class become one.
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

        var previous: String? = null
        classified.forEachIndexed { idx, label ->
            val tag = label.get(CoreAnnotations.AnswerAnnotation::class.java)
                ?.takeUnless { it.isEmpty() || it == classifier.backgroundSymbol() }
            if (tag != null) {
                val ne = if (mapTags) mapTag(tag, continuesEntity = tag == previous) else tag
                morphoMap?.set(sentenceTokenOffsets[idx], KorapXmlTool.MorphoSpan(ne = ne))
            }
            previous = tag
        }
    }

    companion object {
        private val TAG_MAPPING = mapOf(
            "PERSON" to "PER",
            "LOCATION" to "LOC",
            "ORGANIZATION" to "ORG",
            "MISC" to "MISC"
        )

        fun mapTag(tag: String, continuesEntity: Boolean = false): String =
            TAG_MAPPING[tag]?.let { (if (continuesEntity) "I-" else "B-") + it } ?: tag
    }
}
