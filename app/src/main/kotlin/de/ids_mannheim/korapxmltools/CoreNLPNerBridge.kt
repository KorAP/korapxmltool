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
 * Class names are normalized to the short CoNLL/ParlaMint/UNER standard (`PER`, `LOC`,
 * `ORG`, `MISC`; unknown tags are kept verbatim). When written to `corenlp/ne.xml`,
 * contiguous entity tokens within a sentence are collapsed into multi-token spans.
 */
class CoreNLPNerBridge(
    override val model: String,
    override val logger: Logger,
    @Suppress("UNUSED_PARAMETER") mapTags: Boolean = false
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
                ?.takeUnless { it.isEmpty() || it == classifier.backgroundSymbol() }
            if (tag != null) {
                val normalized = normalizeTag(tag)
                if (normalized != null) {
                    morphoMap?.set(sentenceTokenOffsets[idx], KorapXmlTool.MorphoSpan(ne = normalized))
                }
            }
        }
    }

    companion object {
        /**
         * Normalize a CoreNLP NER tag to the short CoNLL/ParlaMint/UNER class name
         * (`PERSON` -> `PER`, `LOCATION` -> `LOC`, `ORGANIZATION` -> `ORG`, `MISC` -> `MISC`),
         * returning null for background ("O") tags. Preserves any `B-`/`I-` prefix if a custom
         * BIO model is used, so downstream span collapsing keeps adjacent entity boundaries.
         */
        fun normalizeTag(tag: String): String? {
            val parsed = NamedEntities.parseTag(tag) ?: return null
            return when (parsed.boundary) {
                NamedEntities.Boundary.BEGIN -> "B-${parsed.label}"
                NamedEntities.Boundary.CONTINUE -> if (parsed.closesAfter) "E-${parsed.label}" else "I-${parsed.label}"
                NamedEntities.Boundary.SINGLE -> "S-${parsed.label}"
                NamedEntities.Boundary.UNMARKED -> parsed.label
            }
        }
    }
}
