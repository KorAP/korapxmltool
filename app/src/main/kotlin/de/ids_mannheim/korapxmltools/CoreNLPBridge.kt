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
    internal data class PosAssignment(val from: Int, val to: Int, val pos: String)

    internal companion object {
        internal fun mergeMorphoPosAssignmentsByExactOffsets(
            assignments: List<PosAssignment>,
            tokens: List<KorapXmlTool.Span>,
            morpho: MutableMap<String, KorapXmlTool.MorphoSpan>,
            logger: Logger
        ) {
            val tokenSpans = tokens.associateBy { "${it.from}-${it.to}" }
            var skippedRetokenizedLeaves = 0

            assignments.forEach { assignment ->
                val spanKey = "${assignment.from}-${assignment.to}"
                if (!tokenSpans.containsKey(spanKey)) {
                    skippedRetokenizedLeaves++
                    return@forEach
                }

                val existing = morpho[spanKey]
                morpho[spanKey] = KorapXmlTool.MorphoSpan(
                    lemma = existing?.lemma ?: "_",
                    upos = existing?.upos ?: "_",
                    xpos = assignment.pos.ifBlank { existing?.xpos ?: "_" },
                    feats = existing?.feats ?: "_",
                    head = existing?.head ?: "_",
                    deprel = existing?.deprel ?: "_",
                    misc = existing?.misc
                )
            }

            if (skippedRetokenizedLeaves > 0) {
                logger.fine(
                    "Skipped $skippedRetokenizedLeaves CoreNLP POS leaf updates without exact token span match"
                )
            }
        }

        internal fun mergeMorphoPosAssignmentsByTokenOrder(
            posTags: List<String>,
            tokens: List<KorapXmlTool.Span>,
            morpho: MutableMap<String, KorapXmlTool.MorphoSpan>
        ) {
            posTags.forEachIndexed { index, pos ->
                if (index >= tokens.size) return@forEachIndexed
                val spanKey = "${tokens[index].from}-${tokens[index].to}"
                val existing = morpho[spanKey]
                morpho[spanKey] = KorapXmlTool.MorphoSpan(
                    lemma = existing?.lemma ?: "_",
                    upos = existing?.upos ?: "_",
                    xpos = pos.ifBlank { existing?.xpos ?: "_" },
                    feats = existing?.feats ?: "_",
                    head = existing?.head ?: "_",
                    deprel = existing?.deprel ?: "_",
                    misc = existing?.misc
                )
            }
        }

        internal fun updateMorphoFromTreeByExactOffsets(
            tree: Tree,
            tokens: List<KorapXmlTool.Span>,
            morpho: MutableMap<String, KorapXmlTool.MorphoSpan>,
            logger: Logger
        ) {
            val assignments = mutableListOf<PosAssignment>()
            val leaves = tree.getLeaves<Tree>()
            for (leafObj in leaves) {
                val leaf = leafObj as Tree
                val parent = leaf.parent(tree) ?: continue
                if (!parent.isPreTerminal) continue

                val label = leaf.label()
                if (label !is HasOffset) continue

                val pos = parent.label()?.value()
                assignments.add(
                    PosAssignment(
                        from = label.beginPosition(),
                        to = label.endPosition(),
                        pos = pos ?: "_"
                    )
                )
            }
            mergeMorphoPosAssignmentsByExactOffsets(assignments, tokens, morpho, logger)
        }
    }

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

        // Use the tool's tokenization and sentence boundaries to keep constituency
        // and morpho output aligned with the input ZIP token layer.
        props.setProperty("tokenize.whitespace", "true")
        props.setProperty("ssplit.eolonly", "true")

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
            sentenceSpans.forEachIndexed { sentenceIdx, sentenceSpan ->
                val sentenceId = "s${sentenceIdx + 1}"
                val sentenceTokens = tokens.filter { token ->
                    token.from >= sentenceSpan.from && token.to <= sentenceSpan.to
                }
                if (sentenceTokens.isEmpty()) {
                    return@forEachIndexed
                }

                val sentenceText = sentenceTokens.joinToString(" ") { token ->
                    text.substring(token.from, token.to)
                }
                val annotation = Annotation(sentenceText)
                pipeline.annotate(annotation)
                val sentences = annotation.get(CoreAnnotations.SentencesAnnotation::class.java)
                if (sentences.isEmpty()) {
                    logger.warning("CoreNLP produced no sentences for sentence $sentenceId")
                    return@forEachIndexed
                }
                val sentence = sentences[0]

                val tree = sentence.get(TreeCoreAnnotations.TreeAnnotation::class.java)

                if (tree != null) {
                    val coreLabels = sentence.get(CoreAnnotations.TokensAnnotation::class.java)
                    if (coreLabels.size != sentenceTokens.size) {
                        logger.warning(
                            "CoreNLP token count mismatch for $sentenceId: parser=${coreLabels.size}, input=${sentenceTokens.size}; " +
                                "skipping constituency tree to avoid misalignment"
                        )
                        return@forEachIndexed
                    }

                    if (coreLabels.isNotEmpty()) {
                        val constituencyTree = convertTree(tree, sentenceId, sentenceTokens)
                        trees.add(constituencyTree)

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
        tokens: List<KorapXmlTool.Span>
    ): ConstituencyTree {
        val nodes = mutableListOf<ConstituencyNode>()
        val rootLeaves = tree.getLeaves<Tree>().map { it as Tree }
        val leafIndices = java.util.IdentityHashMap<Tree, Int>()
        rootLeaves.forEachIndexed { index, leaf -> leafIndices[leaf] = index }

        // Recursively convert tree nodes
        convertNode(tree, tree, sentenceId, tokens, nodes, leafIndices)

        return ConstituencyTree(sentenceId, nodes)
    }

    private fun convertNode(
        node: Tree,
        root: Tree,
        sentenceId: String,
        tokens: List<KorapXmlTool.Span>,
        nodes: MutableList<ConstituencyNode>,
        leafIndices: java.util.IdentityHashMap<Tree, Int>
    ) {
        val nodeNumber = node.nodeNumber(root)
        val nodeId = "${sentenceId}_n${nodeNumber}"

        // Map constituent span boundaries back to the original token spans.
        val leaves: List<Tree> = node.getLeaves<Tree>().map { it as Tree }
        if (leaves.isEmpty()) return

        val firstLeafIndex = leafIndices[leaves.first()] ?: return
        val lastLeafIndex = leafIndices[leaves.last()] ?: return
        if (firstLeafIndex >= tokens.size || lastLeafIndex >= tokens.size) {
            logger.warning("CoreNLP leaf/token index mismatch in $sentenceId: $firstLeafIndex..$lastLeafIndex vs ${tokens.size} tokens")
            return
        }

        val from = tokens[firstLeafIndex].from
        val to = tokens[lastLeafIndex].to

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
                convertNode(child, root, sentenceId, tokens, nodes, leafIndices)
            }
        }
    }

    private fun updatePOSFromTree(
        tree: Tree,
        tokens: List<KorapXmlTool.Span>,
        morpho: MutableMap<String, KorapXmlTool.MorphoSpan>
    ) {
        val posTags = tree.getLeaves<Tree>().map { leafObj ->
            val leaf = leafObj as Tree
            val parent = leaf.parent(tree)
            if (parent != null && parent.isPreTerminal) parent.label()?.value() ?: "_"
            else "_"
        }
        mergeMorphoPosAssignmentsByTokenOrder(posTags, tokens, morpho)
    }
}
