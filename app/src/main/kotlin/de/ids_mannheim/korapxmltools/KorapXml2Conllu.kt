package de.ids_mannheim.korapxmltools

import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import picocli.CommandLine
import picocli.CommandLine.*
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.logging.Level
import java.util.logging.Logger
import java.util.stream.IntStream
import java.util.zip.ZipFile
import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory
import kotlin.math.min
import kotlin.system.exitProcess

@Command(
    name = "KorapXml2Conllu",
    mixinStandardHelpOptions = true,
    version = ["KorapXml2Conllu 2.0-alpha-01"],
    description = ["Converts KorAP-XML <https://github.com/KorAP/KorAP-XML-Krill#about-korap-xml> base or " +
            "morpho zips to CoNLL(-U) format with all information necessary for " +
            "reconstruction in comment lines."]
)

class KorapXml2Conllu : Callable<Int> {
    val COMPATIBILIY_MODE = System.getenv("COMPATIBILIY_MODE") != null

    @Parameters(arity = "1..*", description = ["At least one zip file name"])
    var zipFileNames: Array<String>? = null

    @Option(
        names = ["--sigle-pattern", "-p"],
        paramLabel = "PATTERN",
        description = ["Extract only documents with sigle matching the pattern (regex)"]
    )
    var siglePattern: String? = null

    @Option(
        names = ["--extract-attributes-regex", "-e"],
        paramLabel = "REGEX",
        description = ["Not yet implemented: extract attributes regex"]
    )
    var extractAttributesRegex: String = ""

    @Option(
        names = ["--s-bounds-from-morpho"], description = ["Not yet implemented: s bounds from morpho"]
    )
    var sBoundsFromMorpho: Boolean = false

    @Option(
        names = ["--log", "-l"],
        paramLabel = "LEVEL",
        description = ["Log level: one of SEVERE, WARNING, INFO, FINE, FINER, FINEST. Default: ${"$"}{DEFAULT-VALUE}])"]
    )
    var logLevel: String = "WARNING"

    @Option(
        names = ["--columns", "-c"],
        paramLabel = "NUMBER",
        description = ["Number of columns. 1 means just the token. Default: ${"$"}{DEFAULT-VALUE}", "Possible values: 1-10"]
    )
    var columns: Int = 10

    @Option(
        names = ["--word2vec", "-w"],
        description = ["Print text in LM training format: tokens separated by space, sentences separated by newline"]
    )
    var lmTrainingData: Boolean = false

    @Option(
        names = ["--token-separator", "-s"],
        paramLabel = "SEPARATOR",
        description = ["Not yet implemented: token separator"]
    )
    var tokenSeparator: String = "\n"

    @Option(names = ["--offsets"], description = ["Not yet implemented: offsets"])
    var offsets: Boolean = false

    @Option(names = ["--comments", "-C"], description = ["Not yet implemented: comments"])
    var comments: Boolean = false

    @Option(
        names = ["--extract-metadata-regex", "-m"],
        paramLabel = "REGEX",
        description = ["Not yet implemented: extract metadata regex"]
    )
    var extractMetadataRegex: MutableList<String> = mutableListOf()

    override fun call(): Int {
        LOGGER.level = try {
            Level.parse(logLevel.uppercase(Locale.getDefault()))
        } catch (e: IllegalArgumentException) {
            LOGGER.warning("Invalid log level: $logLevel. Defaulting to WARNING.")
            Level.WARNING
        }

        LOGGER.info("Processing zip files: " + zipFileNames!!.joinToString(", "))

        korapxml2conllu(zipFileNames!!)
        return 0
    }

    private val LOGGER: Logger = Logger.getLogger(KorapXml2Conllu::class.java.name)

    fun korapxml2conllu(args: Array<String>) {
        val executor: ExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())
        val texts: ConcurrentHashMap<String, String> = ConcurrentHashMap()
        val sentences: ConcurrentHashMap<String, Array<Span>> = ConcurrentHashMap()
        val tokens: ConcurrentHashMap<String, Array<Span>> = ConcurrentHashMap()
        val morpho: ConcurrentHashMap<String, MutableMap<String, MorphoSpan>> = ConcurrentHashMap()
        val fnames: ConcurrentHashMap<String, String> = ConcurrentHashMap()

        if (args.isEmpty()) {
            LOGGER.severe("Usage: KorapXml2Conllu <zipfile1> [<zipfile2> ...]")
            return
        }
        var zips: Array<String> = args
        if (args.size == 1 && args[0].matches(Regex(".*\\.([^/.]+)\\.zip$"))) {
            val baseZip = args[0].replace(Regex("\\.([^/.]+)\\.zip$"), ".zip")
            if (File(baseZip).exists()) {
                zips = arrayOf(baseZip, zips[0])
                LOGGER.info("Processing base zip file: $baseZip")
            }
        }
        Arrays.stream(zips).forEach { zipFilePath ->
            executor.submit {
                processZipFile(
                    (zipFilePath ?: "").toString(),
                    texts,
                    sentences,
                    tokens,
                    fnames,
                    morpho,
                    getFoundryFromZipFileNames(zips),
                    zips.size > 1
                )
            }
        }

        executor.shutdown()
        while (!executor.isTerminated) {
            // Wait for all tasks to finish
        }
        texts.keys.sorted().parallelStream().forEach { docId ->
            if (!tokens.containsKey(docId)) {
                tokens[docId] = getTokenSpansFromMorho(morpho[docId]!!)
            }
            processText(
                tokens,
                docId,
                sentences,
                texts,
                getFoundryFromZipFileName(fnames[docId]!!),
                fnames,
                true,
                morpho
            )
        }
    }


    private fun getTokenSpansFromMorho(morpho: MutableMap<String, MorphoSpan>): Array<Span> {
        return morpho.keys.map { key ->
            val fromTo = key.split("-")
            Span(fromTo[0].toInt(), fromTo[1].toInt())
        }.sortedBy {
            it.from
        }.toTypedArray()
    }

    private fun getFoundryFromZipFileName(zipFileName: String): String {
        if (!zipFileName.matches(Regex(".*\\.([^/.]+)\\.zip$"))) {
            return "base"
        }
        return zipFileName.replace(Regex(".*\\.([^/.]+)\\.zip$"), "$1")
    }

    private fun getFoundryFromZipFileNames(zipFileNames: Array<String>): String {
        for (zipFileName in zipFileNames) {
            val foundry = getFoundryFromZipFileName(zipFileName)
            if (foundry != "base") {
                return foundry
            }
        }
        return "base"
    }

    private fun processZipFile(
        zipFilePath: String,
        texts: ConcurrentHashMap<String, String>,
        sentences: ConcurrentHashMap<String, Array<Span>>,
        tokens: ConcurrentHashMap<String, Array<Span>>,
        fname: ConcurrentHashMap<String, String>,
        morpho: ConcurrentHashMap<String, MutableMap<String, MorphoSpan>>,
        foundry: String = "base",
        waitForMorpho: Boolean = false,
    ) {
        try {
            var waitForMorpho = waitForMorpho
            ZipFile(zipFilePath).use { zipFile ->
                zipFile.stream().parallel().forEach { zipEntry ->
                    try {
                        if (zipEntry.name.matches(Regex(".*(data|tokens|structure|morpho)\\.xml$"))) {
                            val inputStream: InputStream = zipFile.getInputStream(zipEntry)
                            val dbFactory: DocumentBuilderFactory = DocumentBuilderFactory.newInstance()
                            val dBuilder: DocumentBuilder = dbFactory.newDocumentBuilder()
                            val doc: Document = dBuilder.parse(InputSource(InputStreamReader(inputStream, "UTF-8")))

                            doc.documentElement.normalize()
                            val docId: String = doc.documentElement.getAttribute("docid")
                            if (siglePattern != null && !Regex(siglePattern!!).containsMatchIn(docId)) {
                                return@forEach
                            }
                            // LOGGER.info("Processing file: " + zipEntry.getName())
                            val fileName = zipEntry.name.replace(Regex(".*?/([^/]+\\.xml)$"), "$1")
                            when (fileName) {
                                "data.xml" -> {
                                    val textsList: NodeList = doc.getElementsByTagName("text")
                                    if (textsList.length > 0) {
                                        texts[docId] = textsList.item(0).textContent
                                    }
                                }

                                "structure.xml" -> {
                                    val spans: NodeList = doc.getElementsByTagName("span")
                                    sentences[docId] = extractSentenceSpans(spans)
                                }

                                "tokens.xml" -> {
                                    if (!fname.contains(docId)) {
                                        fname[docId] = zipEntry.name
                                    }
                                    val tokenSpans: NodeList = doc.getElementsByTagName("span")
                                    tokens[docId] = extractSpans(tokenSpans)
                                }

                                "morpho.xml" -> {
                                    waitForMorpho = true
                                    fname[docId] = zipEntry.name
                                    val fsSpans: NodeList = doc.getElementsByTagName("span")
                                    morpho[docId] = extractMorphoSpans(fsSpans)
                                    if (!tokens.containsKey(docId) && sBoundsFromMorpho) {
                                        tokens[docId] = extractSpans(fsSpans)
                                    }
                                }
                            }

                            if (texts[docId] != null && sentences[docId] != null && tokens[docId] != null && (!waitForMorpho || morpho[docId] != null)) {
                                processText(tokens, docId, sentences, texts, foundry, fname, waitForMorpho, morpho)

                            }
                        }
                    } catch (e: Exception) {
                        e.printStackTrace()
                    }
                }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    private fun processText(
        tokens: ConcurrentHashMap<String, Array<Span>>,
        docId: String,
        sentences: ConcurrentHashMap<String, Array<Span>>,
        texts: ConcurrentHashMap<String, String>,
        foundry: String,
        fname: ConcurrentHashMap<String, String>,
        waitForMorpho: Boolean,
        morpho: ConcurrentHashMap<String, MutableMap<String, MorphoSpan>>
    ) {
        var token_index = 0
        var real_token_index = 0
        var sentence_index = 0
        val output: StringBuilder
        if (lmTrainingData) {
            output = StringBuilder()

            tokens[docId]?.forEach { span ->
                token_index++
                if (span.from >= sentences[docId]!![sentence_index].to) {
                    output.append("\n")
                    sentence_index++
                }
                output.append(texts[docId]!!.substring(span.from, span.to), " ")
                real_token_index++
            }
        } else {
            output =
                StringBuilder("# foundry = $foundry\n# filename = ${fname[docId]}\n# text_id = $docId\n").append(
                    tokenOffsetsInSentence(
                        sentences, docId, sentence_index, real_token_index, tokens
                    )
                )
            tokens[docId]?.forEach { span ->
                token_index++
                if (span.from >= sentences[docId]!![sentence_index].to) {
                    output.append("\n")
                    sentence_index++
                    token_index = 1
                    output.append(
                        tokenOffsetsInSentence(
                            sentences, docId, sentence_index, real_token_index, tokens
                        )
                    )
                }
                if (waitForMorpho && morpho[docId]?.containsKey("${span.from}-${span.to}") == true) {
                    val mfs = morpho[docId]!!["${span.from}-${span.to}"]
                    output.append(
                        printConlluToken(
                            token_index,
                            texts[docId]!!.substring(span.from, span.to),
                            mfs!!.lemma!!,
                            mfs.upos!!,
                            mfs.xpos!!,
                            mfs.feats!!,
                            mfs.head!!,
                            mfs.deprel!!,
                            mfs.deps!!,
                            mfs.misc!!,
                            columns
                        )
                    )
                } else {
                    output.append(
                        printConlluToken(
                            token_index, texts[docId]!!.substring(span.from, span.to), columns = columns
                        )
                    )
                }
                real_token_index++
            }
        }
        synchronized(System.out) {
            println(output.toString())
        }

        arrayOf(tokens, texts, sentences, morpho, fname).forEach { map ->
            map.remove(docId)
        }
    }


    private fun printConlluToken(
        token_index: Int,
        token: String,
        lemma: String = "_",
        upos: String = "_",
        xpos: String = "_",
        feats: String = "_",
        head: String = "_",
        deprel: String = "_",
        deps: String = "_",
        misc: String = "_",
        columns: Int = 10
    ): String {
        val myUpos = if (COMPATIBILIY_MODE && upos == "_") xpos else upos
        when (columns) {
            1 -> return ("$token\n")
            10 -> return ("$token_index\t$token\t$lemma\t$myUpos\t$xpos\t$feats\t$head\t$deprel\t$deps\t$misc\n")
            else -> return arrayOf(token_index, token, lemma, myUpos, xpos, feats, head, deprel, deps, misc).slice(0..min(columns, 10) - 1)
                .joinToString("\t") + "\n"
        }
    }

    private fun tokenOffsetsInSentence(
        sentences: ConcurrentHashMap<String, Array<Span>>,
        docId: String,
        sentence_index: Int,
        token_index: Int,
        tokens: ConcurrentHashMap<String, Array<Span>>
    ): String {
        val sentenceEndOffset: Int
        sentenceEndOffset = if (sentences[docId] == null) {
            -1
        } else {
            sentences[docId]!![sentence_index].to
        }
        var i = token_index
        val start_offsets_string = StringBuilder()
        val end_offsets_string = StringBuilder()
        while (tokens[docId] != null && i < tokens[docId]!!.size && tokens[docId]!![i].to <= sentenceEndOffset) {
            start_offsets_string.append(" ", tokens[docId]!![i].from)
            end_offsets_string.append(" ", tokens[docId]!![i].to)
            i++
        }
        return (
                StringBuilder() .append(
                    "# start_offsets = ", tokens[docId]!![token_index].from, start_offsets_string, "\n",
                    "# end_offsets = ", sentenceEndOffset, end_offsets_string, "\n"
                ).toString())
    }

    private fun extractSpans(spans: NodeList): Array<Span> {
        return IntStream.range(0, spans.length).mapToObj(spans::item).filter { node -> node is Element }.map { node ->
                Span(
                    Integer.parseInt((node as Element).getAttribute("from")), Integer.parseInt(node.getAttribute("to"))
                )
            }.toArray { size -> arrayOfNulls(size) }
    }

    private fun extractMorphoSpans(
        fsSpans: NodeList
    ): MutableMap<String, MorphoSpan> {
        val res: MutableMap<String, MorphoSpan> = HashMap()
        IntStream.range(0, fsSpans.length).mapToObj(fsSpans::item).filter { node -> node is Element && node.getAttribute("type") != "alt" }.forEach { node ->
                val features = (node as Element).getElementsByTagName("f")
                val fs = MorphoSpan()
                val fromTo = "${node.getAttribute("from")}-${node.getAttribute("to")}"
                IntStream.range(0, features.length).mapToObj(features::item).forEach { feature ->
                        val attr = (feature as Element).getAttribute("name")
                        val value = feature.textContent.trim()
                        if (value.isEmpty()) return@forEach
                        when (attr) {
                            "lemma" -> if(fs.lemma == "_") fs.lemma = value
                            "upos" -> fs.upos = value
                            "xpos", "ctag", "pos" -> if(fs.xpos == "_") fs.xpos = value
                            "feats", "msd" -> if(fs.feats == "_" ) fs.feats = value
                            "type" -> if(fs.feats == "_") fs.feats = feature.getElementsByTagName("symbol").item(0).attributes.getNamedItem("value").textContent.trim()
                            // "subtype" -> if(fs.feats == "_") fs.feats += ":" + feature.getElementsByTagName("symbol").item(0).attributes.getNamedItem("value").textContent
                            "certainty" -> fs.misc = value
                        }
                    }
                res[fromTo] = fs
            }
        return res
    }

    private fun extractSentenceSpans(spans: NodeList): Array<Span> {
        return IntStream.range(0, spans.length).mapToObj(spans::item)
            .filter { node -> node is Element && node.getElementsByTagName("f").item(0).textContent.equals("s") }
            .map { node ->
                Span(
                    Integer.parseInt((node as Element).getAttribute("from")), Integer.parseInt(node.getAttribute("to"))
                )
            }.toArray { size -> arrayOfNulls(size) }
    }


    internal class Span(var from: Int, var to: Int)

    internal class MorphoSpan(
        var lemma: String? = "_",
        var upos: String? = "_",
        var xpos: String? = "_",
        var feats: String? = "_",
        var head: String? = "_",
        var deprel: String? = "_",
        var deps: String? = "_",
        var misc: String? = "_"
    )

}

fun main(args: Array<String>): Unit = exitProcess(CommandLine(KorapXml2Conllu()).execute(*args))

fun debug(args: Array<String>): Int {
    return (CommandLine(KorapXml2Conllu()).execute(*args))
}
