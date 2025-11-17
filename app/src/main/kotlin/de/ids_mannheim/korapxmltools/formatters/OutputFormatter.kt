package de.ids_mannheim.korapxmltools.formatters

import de.ids_mannheim.korapxmltools.KorapXmlTool
import de.ids_mannheim.korapxmltools.NonBmpString

/**
 * Common data structure passed to all output formatters.
 * Contains all the document data that might be needed by any formatter.
 */
data class OutputContext(
    val docId: String,
    val foundry: String,
    val tokens: Array<KorapXmlTool.Span>?,
    val sentences: Array<KorapXmlTool.Span>?,
    val text: NonBmpString?,
    val morpho: MutableMap<String, KorapXmlTool.MorphoSpan>?,
    val metadata: Array<String>?,
    val extraFeatures: MutableMap<String, String>?,
    val fileName: String?,
    val useLemma: Boolean,
    val extractMetadataRegex: List<String>,
    val columns: Int = 10
)

/**
 * Base interface for all output formatters.
 */
interface OutputFormatter {
    /**
     * Format the given document data and return the output as a StringBuilder.
     */
    fun format(context: OutputContext): StringBuilder
    
    /**
     * Get the name of this output format (e.g., "word2vec", "conllu")
     */
    val formatName: String
}
