package de.ids_mannheim.korapxmltools

abstract class ParserToolBridge : AnnotationToolBridge {
    fun parseText(
        tokens: Array<KorapXmlTool.Span>,
        morpho: MutableMap<String, KorapXmlTool.MorphoSpan>?,
        sentenceSpans: Array<KorapXmlTool.Span>?,
        text: NonBmpString
    ): MutableMap<String, KorapXmlTool.MorphoSpan> {
        val sentence_tokens = mutableListOf<String>()
        val sentence_token_offsets = mutableListOf<String>()
        var token_index = 1
        var sentence_index = 0
        tokens.forEach { span ->
            if (span.from >= (sentenceSpans?.get(sentence_index)?.to ?: 11111110)) {
                tagSentence(sentence_tokens, sentence_token_offsets, morpho)
                sentence_tokens.clear()
                sentence_token_offsets.clear()
                sentence_index++
                token_index = 1

            }
            sentence_tokens.add(
                "$token_index\t${
                    text.substring(
                        span.from, span.to
                    )
                }\t_\t${morpho?.get("${span.from}-${span.to}")?.xpos ?: "_"}\t${morpho?.get("${span.from}-${span.to}")?.xpos ?: "_"}\t${
                    morpho?.get(
                        "${span.from}-${span.to}"
                    )?.feats ?: "_"
                }\t_\t_\t_\t_"
            )
            sentence_token_offsets.add("${span.from}-${span.to}")
            token_index++
        }
        if (sentence_tokens.size > 0) {
            try {
                tagSentence(sentence_tokens, sentence_token_offsets, morpho)
            } catch (e: ArrayIndexOutOfBoundsException) {
                logger.warning("Tagging failed: ${e.message} ${e.stackTrace} ${sentence_tokens.joinToString { " " }}")
            }
        }
        return morpho!!
    }
}