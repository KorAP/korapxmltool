package de.ids_mannheim.korapxmltools

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.util.CoreMap
import java.io.File
import java.util.*
import java.util.logging.Logger

class CoreNLPTaggerBridge(override val model: String, override val logger: Logger) : TaggerToolBridge() {
    override val foundry = "corenlp"

    private val pipeline: StanfordCoreNLP

    init {
        logger.info("Initializing CoreNLP tagger with model $model")
        val props = Properties()

        // Basic annotators for POS tagging
        props.setProperty("annotators", "tokenize,ssplit,pos")

        // Set the POS model from the model parameter
        if (File(model).exists()) {
            props.setProperty("pos.model", model)
            logger.info("Loading POS model from $model")
        } else {
            throw IllegalArgumentException("Model file not found: $model")
        }

        // Configure for German if model name suggests it
        if (model.contains("german") || model.contains("German")) {
            logger.info("Detected German model")
        }

        // Use whitespace tokenization since we have our own tokens
        props.setProperty("tokenize.whitespace", "true")
        props.setProperty("ssplit.eolonly", "true")

        pipeline = StanfordCoreNLP(props)
        logger.info("CoreNLP tagger initialized successfully")
    }

    @Throws(java.lang.ArrayIndexOutOfBoundsException::class, java.lang.Exception::class)
    override fun tagSentence(
        sentenceTokens: MutableList<String>,
        sentenceTokenOffsets: MutableList<String>,
        morphoMap: MutableMap<String, KorapXmlTool.MorphoSpan>?
    ) {
        if (sentenceTokens.isEmpty()) return

        // Build sentence text from tokens
        val sentenceText = sentenceTokens.joinToString(" ")

        try {
            // Annotate with CoreNLP
            val annotation = Annotation(sentenceText)
            pipeline.annotate(annotation)

            // Get the annotated sentence
            val sentences: List<CoreMap> = annotation.get(CoreAnnotations.SentencesAnnotation::class.java)
            if (sentences.isEmpty()) {
                logger.warning("CoreNLP produced no sentences for: $sentenceText")
                return
            }

            val sentence = sentences[0]
            val tokens: List<CoreLabel> = sentence.get(CoreAnnotations.TokensAnnotation::class.java)

            // Map POS tags back to our tokens
            tokens.forEachIndexed { idx, token ->
                if (idx < sentenceTokenOffsets.size) {
                    val pos = token.get(CoreAnnotations.PartOfSpeechAnnotation::class.java)
                    val lemma = token.get(CoreAnnotations.LemmaAnnotation::class.java)

                    val taggedWord = KorapXmlTool.MorphoSpan(
                        lemma = lemma ?: "_",
                        xpos = pos ?: "_",
                        upos = "_",  // CoreNLP doesn't provide universal POS by default
                        feats = "_"
                    )
                    morphoMap?.set(sentenceTokenOffsets[idx], taggedWord)
                }
            }
        } catch (e: Exception) {
            logger.warning("Failed to tag sentence: ${e.message}")
            throw e
        }
    }
}
