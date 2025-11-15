package de.ids_mannheim.korapxmltools

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.HasOffset
import edu.stanford.nlp.ling.Label
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.trees.Tree
import edu.stanford.nlp.trees.TreeCoreAnnotations
import edu.stanford.nlp.util.CoreMap
import java.io.File
import java.util.*
import java.util.logging.Logger

class CoreNLPBridge(override val model: String, override val logger: Logger, val taggerModel: String? = null) : ConstituencyParserBridge() {
    override val foundry = "corenlp"

    private val pipeline: StanfordCoreNLP

    init {
        logger.info("Initializing CoreNLP parser with model $model" + (if (taggerModel != null) " and tagger model $taggerModel" else ""))
        val props = Properties()

        // Basic annotators for constituency parsing
        // tokenize and ssplit are needed, but we'll provide our own sentence splitting
        props.setProperty("annotators", "tokenize,ssplit,pos,parse")

        // Set the parse model from the model parameter
        if (File(model).exists()) {
            props.setProperty("parse.model", model)
            logger.info("Loading parse model from $model")
        } else {
            throw IllegalArgumentException("Parser model file not found: $model")
        }

        // Set the POS model - use taggerModel if provided, otherwise try default
        if (taggerModel != null && File(taggerModel).exists()) {
            props.setProperty("pos.model", taggerModel)
            logger.info("Loading POS model from $taggerModel for parser")
        } else if (model.contains("german") || model.contains("German") || model.contains("SR")) {
            // German-specific settings - use built-in model
            props.setProperty("pos.model", "edu/stanford/nlp/models/pos-tagger/german/german-hgc.tagger")
            logger.info("Using default German POS model for parser")
        } else {
            logger.warning("No POS model specified for parser - CoreNLP may fail. Use both -t and -P with corenlp.")
        }

        // Use default sentence splitting (not eolonly)
        // tokenize.whitespace=true would prevent CoreNLP from retokenizing, but we want it to
        // retokenize for better parse quality

        pipeline = StanfordCoreNLP(props)
        logger.info("CoreNLP parser initialized successfully")
    }

    override fun parseConstituency(
        tokens: Array<KorapXmlTool.Span>,
        morpho: MutableMap<String, KorapXmlTool.MorphoSpan>?,
        sentenceSpans: Array<KorapXmlTool.Span>?,
        text: NonBmpString
    ): List<ConstituencyTree> {
        val trees = mutableListOf<ConstituencyTree>()

        if (sentenceSpans == null || sentenceSpans.isEmpty()) {
            logger.warning("No sentence spans provided for constituency parsing")
            return trees
        }

        try {
            // Annotate the ENTIRE document text at once, like the Java implementation does
            // This ensures CoreNLP gives us document-level offsets directly
            val docText = text.toString()
            val annotation = Annotation(docText)
            pipeline.annotate(annotation)

            // Get all sentences from CoreNLP
            val sentences = annotation.get(CoreAnnotations.SentencesAnnotation::class.java)

            if (sentences.isEmpty()) {
                logger.warning("CoreNLP produced no sentences")
                return trees
            }

            // Process each sentence
            sentences.forEachIndexed { sentenceIdx, sentence ->
                val sentenceId = "s${sentenceIdx + 1}"

                val tree = sentence.get(TreeCoreAnnotations.TreeAnnotation::class.java)

                if (tree != null) {
                    // Get tokens for this sentence based on CoreNLP's sentence boundaries
                    val coreLabels = sentence.get(CoreAnnotations.TokensAnnotation::class.java)
                    if (coreLabels.isNotEmpty()) {
                        val sentStart = coreLabels[0].beginPosition()
                        val sentEnd = coreLabels[coreLabels.size - 1].endPosition()

                        val sentenceTokens = tokens.filter { token ->
                            token.from >= sentStart && token.to <= sentEnd
                        }

                        // Convert Stanford tree to our ConstituencyTree format
                        // No offset adjustment needed since CoreNLP already has document-level offsets
                        val constituencyTree = convertTree(tree, sentenceId, sentenceTokens, 0)
                        trees.add(constituencyTree)

                        // Optionally update morpho with POS tags from CoreNLP
                        if (morpho != null) {
                            updatePOSFromTree(tree, sentenceTokens, morpho)
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger.warning("Failed to parse document: ${e.message}")
            e.printStackTrace()
        }

        return trees
    }

    private fun convertTree(
        tree: Tree,
        sentenceId: String,
        tokens: List<KorapXmlTool.Span>,
        sentenceOffsetInDoc: Int
    ): ConstituencyTree {
        val nodes = mutableListOf<ConstituencyNode>()

        // Recursively convert tree nodes
        // We need to pass offset adjustment since CoreNLP gives offsets relative to sentence text
        convertNode(tree, tree, sentenceId, tokens, nodes, sentenceOffsetInDoc)

        return ConstituencyTree(sentenceId, nodes)
    }

    private fun convertNode(
        node: Tree,
        root: Tree,
        sentenceId: String,
        tokens: List<KorapXmlTool.Span>,
        nodes: MutableList<ConstituencyNode>,
        sentenceOffsetInDoc: Int
    ) {
        val nodeNumber = node.nodeNumber(root)
        val nodeId = "${sentenceId}_n${nodeNumber}"

        // Get character offsets from leaves
        val leaves: List<Tree> = node.getLeaves()
        if (leaves.isEmpty()) return

        val firstLeafLabel: Label = leaves[0].label()
        val lastLeafLabel: Label = leaves[leaves.size - 1].label()

        // Get offsets from the leaf labels
        // CoreNLP gives offsets relative to the sentence text we fed it,
        // so we need to add sentenceOffsetInDoc to get document-level offsets
        val from: Int
        val to: Int

        if (firstLeafLabel is HasOffset && lastLeafLabel is HasOffset) {
            from = (firstLeafLabel as HasOffset).beginPosition() + sentenceOffsetInDoc
            to = (lastLeafLabel as HasOffset).endPosition() + sentenceOffsetInDoc
        } else {
            // Fallback: use first and last tokens from sentence
            from = tokens[0].from
            to = tokens[tokens.size - 1].to
        }

        // Get children
        val children = mutableListOf<ConstituencyChild>()
        for (child in node.children()) {
            if (!child.isLeaf()) {
                val childNumber = child.nodeNumber(root)
                val childId = "${sentenceId}_n${childNumber}"

                if (child.isPreTerminal) {
                    // Points to morpho.xml
                    children.add(ConstituencyChild.MorphoRef(childId))
                } else {
                    // Points to another constituent node
                    children.add(ConstituencyChild.NodeRef(childId))
                }
            }
        }

        nodes.add(
            ConstituencyNode(
                id = nodeId,
                label = node.value() ?: "UNKNOWN",
                from = from,
                to = to,
                children = children
            )
        )

        // Recursively process children
        for (child in node.children()) {
            if (!child.isLeaf()) {
                convertNode(child, root, sentenceId, tokens, nodes, sentenceOffsetInDoc)
            }
        }
    }

    private fun updatePOSFromTree(
        tree: Tree,
        tokens: List<KorapXmlTool.Span>,
        morpho: MutableMap<String, KorapXmlTool.MorphoSpan>
    ) {
        // Get POS tags from pre-terminal nodes
        val leaves: List<Tree> = tree.getLeaves()
        leaves.forEachIndexed { idx, leaf: Tree ->
            if (idx < tokens.size) {
                val parent: Tree? = leaf.parent(tree)
                if (parent != null && parent.isPreTerminal()) {
                    val pos: String? = parent.label()?.value()
                    val spanKey = "${tokens[idx].from}-${tokens[idx].to}"
                    val existing = morpho[spanKey]

                    // Update or create morpho entry with POS tag
                    morpho[spanKey] = KorapXmlTool.MorphoSpan(
                        lemma = existing?.lemma ?: "_",
                        upos = existing?.upos ?: "_",
                        xpos = pos ?: existing?.xpos ?: "_",
                        feats = existing?.feats ?: "_",
                        head = existing?.head ?: "_",
                        deprel = existing?.deprel ?: "_",
                        misc = existing?.misc
                    )
                }
            }
        }
    }
}
