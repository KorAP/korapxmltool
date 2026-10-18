package de.ids_mannheim.korapxmltools

import edu.stanford.nlp.ling.HasWord
import edu.stanford.nlp.ling.Word
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import java.io.File
import java.util.logging.Logger

class CoreNLPTaggerBridge(override val model: String, override val logger: Logger) : TaggerToolBridge() {
    override val foundry = "corenlp"

    private val tagger: MaxentTagger

    init {
        logger.info("Initializing CoreNLP tagger with model $model")

        if (!File(model).exists()) {
            throw IllegalArgumentException("Model file not found: $model")
        }

        // Use the MaxentTagger directly on our tokens instead of a StanfordCoreNLP pipeline:
        // even its whitespace tokenizer splits tokens at characters like U+2009 or U+000B and
        // drops whitespace-only tokens, which shifted all following POS tags in the sentence.
        tagger = MaxentTagger(model)
        logger.info("CoreNLP tagger initialized successfully")
    }

    @Throws(java.lang.ArrayIndexOutOfBoundsException::class, java.lang.Exception::class)
    override fun tagSentence(
        sentenceTokens: MutableList<String>,
        sentenceTokenOffsets: MutableList<String>,
        morphoMap: MutableMap<String, KorapXmlTool.MorphoSpan>?
    ) {
        if (sentenceTokens.isEmpty()) return

        try {
            val words: List<HasWord> = sentenceTokens.map { Word(it) }
            val taggedWords = tagger.tagSentence(words)

            if (taggedWords.size != sentenceTokens.size) {
                logger.warning(
                    "CoreNLP tagger token count mismatch: tagger=${taggedWords.size}, input=${sentenceTokens.size}; " +
                        "skipping sentence to avoid misalignment"
                )
                return
            }

            taggedWords.forEachIndexed { idx, taggedWord ->
                morphoMap?.set(
                    sentenceTokenOffsets[idx],
                    KorapXmlTool.MorphoSpan(
                        lemma = "_",  // the CoreNLP tagger does not lemmatize
                        xpos = taggedWord.tag() ?: "_",
                        upos = "_",  // CoreNLP doesn't provide universal POS by default
                        feats = "_"
                    )
                )
            }
        } catch (e: Exception) {
            logger.warning("Failed to tag sentence: ${e.message}")
            throw e
        }
    }
}
