package de.ids_mannheim.korapxmltools

abstract class TaggerToolBridge : AnnotationToolBridge {

    fun tagText(
        tokens: Array<KorapXmlTool.Span>, sentenceSpans: Array<KorapXmlTool.Span>?, text: NonBmpString
    ): MutableMap<String, KorapXmlTool.MorphoSpan> {
        val sentence_tokens = mutableListOf<String>()
        val sentence_token_offsets = mutableListOf<String>()
        val morphoMap = mutableMapOf<String, KorapXmlTool.MorphoSpan>()
        var token_index = 0
        var sentence_index = 0
        tokens.forEach { span ->
            if (span.from >= (sentenceSpans?.get(sentence_index)?.to ?: 11111110)) {
                tagSentence(sentence_tokens, sentence_token_offsets, morphoMap)
                sentence_tokens.clear()
                sentence_token_offsets.clear()
                sentence_index++
                token_index = 1

            }
            sentence_tokens.add(text.substring(span.from, span.to))
            sentence_token_offsets.add("${span.from}-${span.to}")
            token_index++
        }
        if (sentence_tokens.size > 0) {
            try {
                tagSentence(sentence_tokens, sentence_token_offsets, morphoMap)
            } catch (e: ArrayIndexOutOfBoundsException) {
                logger.warning("Tagging failed: ${e.message} ${e.stackTrace} ${sentence_tokens.joinToString { " " }}")
            }
        }
        return morphoMap
    }
}