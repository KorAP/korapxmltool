package de.ids_mannheim.korapxmltools.formatters

/**
 * Formatter for Word2Vec / language model training output.
 * Outputs tokens in lemmatized form (or word form if no lemma), space-separated,
 * with sentences separated by newlines.
 */
object Word2VecFormatter : OutputFormatter {
    override val formatName: String = "word2vec"

    override fun format(context: OutputContext): StringBuilder {
        var tokenIndex = 0
        var realTokenIndex = 0
        var sentenceIndex = 0
        val output = StringBuilder()
        
        // Prepend metadata if requested
        if (context.extractMetadataRegex.isNotEmpty()) {
            output.append(context.metadata?.joinToString("\t", postfix = "\t") ?: "")
        }
        
        // Handle case where text is not available (lemma-only mode)
        if (context.text == null) {
            context.tokens?.forEach { span ->
                val key = "${span.from}-${span.to}"
                val lemmaVal = context.morpho?.get(key)?.lemma
                output.append((lemmaVal?.takeIf { it != "_" } ?: "_"), " ")
            }
            if (output.isNotEmpty()) output.deleteCharAt(output.length - 1)
            return output
        }
        
        // Main processing with text available
        context.tokens?.forEach { span ->
            tokenIndex++
            
            // Check if we're starting a new sentence
            if (context.sentences != null && 
                (sentenceIndex >= context.sentences.size || span.from >= context.sentences[sentenceIndex].to)) {
                // Replace trailing space with newline to end previous sentence
                if (output.isNotEmpty()) {
                    output.setCharAt(output.length - 1, '\n')
                } else {
                    output.append("\n")
                }
                
                // Add metadata for new sentence if requested
                if (context.extractMetadataRegex.isNotEmpty() && realTokenIndex < context.tokens.size - 1) {
                    output.append(context.metadata?.joinToString("\t", postfix = "\t") ?: "")
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
        if (output.isNotEmpty()) output.deleteCharAt(output.length - 1)
        
        return output
    }
}
