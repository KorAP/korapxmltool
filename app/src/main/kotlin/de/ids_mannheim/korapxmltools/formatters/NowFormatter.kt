package de.ids_mannheim.korapxmltools.formatters

/**
 * Formatter for NOW corpus export format.
 * Similar to Word2Vec but with:
 * - Document ID prefix: @@<textSigle>
 * - Sentence delimiter: <p>
 * Can use lemmas instead of surface forms when available.
 */
object NowFormatter : OutputFormatter {
    override val formatName: String = "now"

    override fun format(context: OutputContext): StringBuilder {
        var tokenIndex = 0
        var realTokenIndex = 0
        var sentenceIndex = 0
        val output = StringBuilder()

        // Prepend document ID
        output.append("@@${context.docId} ")

        // Handle case where text is not available (lemma-only mode)
        if (context.text == null) {
            context.tokens?.forEach { span ->
                // Check for sentence boundaries
                if (context.sentences != null && 
                    (sentenceIndex >= context.sentences.size || span.from >= context.sentences[sentenceIndex].to)) {
                    // Add sentence delimiter if not at start
                    if (output.isNotEmpty() && !output.endsWith("@@${context.docId} ")) {
                        output.append(" <p> ")
                    }
                    sentenceIndex++
                }
                
                val key = "${span.from}-${span.to}"
                val lemmaVal = context.morpho?.get(key)?.lemma
                output.append((lemmaVal?.takeIf { it != "_" } ?: "_"), " ")
            }
            if (output.isNotEmpty() && output.endsWith(" ")) {
                output.deleteCharAt(output.length - 1)
            }
            return output
        }

        // Main processing with text available
        context.tokens?.forEach { span ->
            tokenIndex++
            
            // Check if we're starting a new sentence
            if (context.sentences != null && 
                (sentenceIndex >= context.sentences.size || span.from >= context.sentences[sentenceIndex].to)) {
                // Add sentence delimiter (but not at the very start after docId)
                if (output.isNotEmpty() && !output.endsWith("@@${context.docId} ")) {
                    output.append(" <p> ")
                }
                sentenceIndex++
            }
            
            // Get safe text boundaries
            val safeFrom = span.from.coerceIn(0, context.text.length)
            val safeTo = span.to.coerceIn(safeFrom, context.text.length)
            
            // Output lemma if available and requested, otherwise surface form
            if (context.useLemma && context.morpho != null) {
                val key = "${span.from}-${span.to}"
                val lemmaVal = context.morpho[key]?.lemma
                if (lemmaVal != null && lemmaVal != "_") {
                    output.append(lemmaVal).append(' ')
                } else {
                    // Fallback to surface form
                    context.text.appendRangeTo(output, safeFrom, safeTo)
                    output.append(' ')
                }
            } else {
                context.text.appendRangeTo(output, safeFrom, safeTo)
                output.append(' ')
            }
            
            realTokenIndex++
        }
        
        // Remove trailing space
        if (output.isNotEmpty() && output.endsWith(" ")) {
            output.deleteCharAt(output.length - 1)
        }
        
        return output
    }
}
