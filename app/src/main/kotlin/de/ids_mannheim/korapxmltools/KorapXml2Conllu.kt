package de.ids_mannheim.korapxmltools

import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory
import java.io.InputStream
import java.util.Arrays
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.stream.IntStream
import java.util.zip.ZipFile
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import picocli.CommandLine
import picocli.CommandLine.Parameters
import picocli.CommandLine.Option
import picocli.CommandLine.Command
import java.io.File
import java.io.InputStreamReader
import java.util.HashMap
import java.util.concurrent.Callable
import java.util.logging.Logger
import kotlin.system.exitProcess

@Command(
    name = "KorapXml2Conllu",
    mixinStandardHelpOptions = true,
    version = ["KorapXml2Conllu 2.0-alpha-01"],
    description = ["Converts KorAP XML files to CoNLL-U format"]
)

class KorapXml2Conllu : Callable<Int> {

    @Parameters(arity = "1..*", description = ["At least one zip file name"])
    var zipFileNames: Array<String>? = null

    @Option(names = ["--sigle-pattern", "-p"], paramLabel = "PATTERN",
        description = ["Not yet implemented: sigle pattern"])
    var siglePattern: String = ""

    @Option(names = ["--extract-attributes-regex", "-e"], paramLabel = "REGEX",
        description = ["Not yet implemented: extract attributes regex"])
    var extractAttributesRegex: String = ""

    @Option(names = ["--s-bounds-from-morpho"],
        description = ["Not yet implemented: s bounds from morpho"])
    var sBoundsFromMorpho: Boolean = false

    @Option(names = ["--log", "-l"], paramLabel = "LEVEL",
        description = ["Not yet implemented: log level"])
    var logLevel: String = "warn"

    @Option(names = ["--columns", "-c"], paramLabel = "NUMBER",
        description = ["Not yet implemented: columns"])
    var columns: Int = 10

    @Option(names = ["--word2vec", "-w"], description = ["Not yet implemented: word2vec"])
    var lmTrainingData: Boolean = false

    @Option(names = ["--token-separator", "-s"], paramLabel = "SEPARATOR",
        description = ["Not yet implemented: token separator"])
    var tokenSeparator: String = "\n"

    @Option(names = ["--offsets"], description = ["Not yet implemented: offsets"])
    var offsets: Boolean = false

    @Option(names = ["--comments"], description = ["Not yet implemented: comments"])
    var comments: Boolean = false

    @Option(names = ["--extract-metadata-regex", "-m"], paramLabel = "REGEX",
        description = ["Not yet implemented: extract metadata regex"])
    var extractMetadataRegex: MutableList<String> = mutableListOf()

    override fun call(): Int {
        LOGGER.info("Processing zip files: " + zipFileNames!!.joinToString(", "))
        korapxml2conllu(zipFileNames!!)// Your application logic here
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

        if (args == null || args.isEmpty() || args[0] == null) {
                LOGGER.severe("Usage: KorapXml2Conllu <zipfile1> [<zipfile2> ...]")
                return
        }
        var zips:Array<String> = args
        if (args.size == 1 && args[0]!!.matches(Regex(".*\\.([^/.]+)\\.zip$")) == true) {
            val baseZip = args[0]!!.replace(Regex("\\.([^/.]+)\\.zip$"), ".zip")
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

        // Further processing as needed
    }

    private fun getFoundryFromZipFileName(zipFileName: String): String {
        if (!zipFileName.matches(Regex(".*\\.([^/.]+)\\.zip$"))) {
            return "base"
        }
        return zipFileName.replace(Regex(".*\\.([^/.]+)\\.zip$"), "$1")
    }

    private fun getFoundryFromZipFileNames(zipFileNames: Array<String>): String {
        for (zipFileName in zipFileNames) {
            val foundry = getFoundryFromZipFileName(zipFileName!!)
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

                            // LOGGER.info("Processing file: " + zipEntry.getName())
                            val fileName =
                                zipEntry.name.replace(Regex(".*?/([^/]+\\.xml)$"), "$1")
                            var token_index = 0
                            var real_token_index = 0
                            var sentence_index = 0
                            when (fileName) {
                                "data.xml" -> {
                                    val textsList: NodeList = doc.getElementsByTagName("text")
                                    if (textsList.length > 0) {
                                        texts[docId] = textsList.item(0).textContent
                                    }
                                }

                                "structure.xml" -> {
                                    val spans: NodeList = doc.getElementsByTagName("span")
                                    val sentenceSpans =
                                        extractSentenceSpans(spans)
                                    sentences[docId] = sentenceSpans
                                }

                                "tokens.xml" -> {
                                    fname[docId] = zipEntry.name
                                    val tokenSpans: NodeList = doc.getElementsByTagName("span")
                                    val tokenSpanObjects =
                                        extractSpans(tokenSpans)
                                    tokens[docId] = tokenSpanObjects
                                }

                                "morpho.xml" -> {
                                    val fsSpans: NodeList = doc.getElementsByTagName("span")
                                    morpho[docId] = extractMorphoSpans(fsSpans)
                                }
                            }
                            if (texts[docId] != null && sentences[docId] != null && tokens[docId] != null
                                && (!waitForMorpho || morpho[docId] != null)
                            ) {
                                synchronized(System.out) {
                                    println("# foundry = $foundry")
                                    println("# filename = ${fname[docId]}")
                                    println("# text_id = $docId")
                                    printTokenOffsetsInSentence(
                                        sentences,
                                        docId,
                                        sentence_index,
                                        real_token_index,
                                        tokens
                                    )
                                    tokens[docId]?.forEach { span ->
                                        token_index++
                                        if (span.from >= sentences[docId]!![sentence_index].to) {
                                            println()
                                            sentence_index++
                                            token_index = 1
                                            printTokenOffsetsInSentence(
                                                sentences,
                                                docId,
                                                sentence_index,
                                                real_token_index,
                                                tokens
                                            )
                                        }
                                        if (waitForMorpho && morpho[docId]?.containsKey("${span.from}-${span.to}") == true) {
                                            val mfs = morpho[docId]!!["${span.from}-${span.to}"]
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
                                                mfs.misc!!
                                            )
                                        } else {
                                            printConlluToken(
                                                token_index, texts[docId]!!.substring(span.from, span.to)
                                            )
                                        }
                                        real_token_index++

                                    }
                                    arrayOf(tokens, texts, sentences, morpho).forEach { map ->
                                        map.remove(docId)
                                    }
                                    println()
                                }

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
        misc: String = "_"
    ) {
        println("$token_index\t$token\t$lemma\t$upos\t$xpos\t$feats\t$head\t$deprel\t$deps\t$misc")
    }

    private fun printTokenOffsetsInSentence(
        sentences: ConcurrentHashMap<String, Array<Span>>,
        docId: String,
        sentence_index: Int,
        token_index: Int,
        tokens: ConcurrentHashMap<String, Array<Span>>
    ) {
        val sentenceEndOffset: Int
        if (sentences[docId] == null) {
            sentenceEndOffset = -1
        } else {
            sentenceEndOffset = sentences[docId]!![sentence_index].to
        }
        var i = token_index
        var start_offsets_string = ""
        var end_offsets_string = ""
        while (tokens[docId]!=null && i < tokens[docId]!!.size && tokens[docId]!![i].to <= sentenceEndOffset) {
            start_offsets_string += " " + tokens[docId]!![i].from
            end_offsets_string += " " + tokens[docId]!![i].to
            i++
        }
        println("# start_offsets = " + tokens[docId]!![token_index].from + start_offsets_string)
        println("# end_offsets = " + sentenceEndOffset + end_offsets_string)
    }

    private fun extractSpans(spans: NodeList): Array<Span> {
        return IntStream.range(0, spans.length)
            .mapToObj(spans::item)
            .filter { node -> node is Element }
            .map { node ->
                Span(
                    Integer.parseInt((node as Element).getAttribute("from")),
                    Integer.parseInt(node.getAttribute("to"))
                )
            }
            .toArray { size -> arrayOfNulls(size) }
    }

    private fun extractMorphoSpans(
        fsSpans: NodeList
    ): MutableMap<String, MorphoSpan> {
        val res: MutableMap<String, MorphoSpan> = HashMap()
        IntStream.range(0, fsSpans.length)
            .mapToObj(fsSpans::item)
            .forEach { node ->
                val features = (node as Element).getElementsByTagName("f")
                val fs = MorphoSpan()
                val fromTo = node.getAttribute("from") + "-" + node.getAttribute("to")
                IntStream.range(0, features.length).mapToObj(features::item)
                    .forEach { feature ->
                        val attr = (feature as Element).getAttribute("name")
                        val value = feature.textContent
                        when (attr) {
                            "lemma" -> fs.lemma = value
                            "upos" -> fs.upos = value
                            "xpos", "ctag", "pos" -> fs.xpos = value
                            "feats", "msd" -> fs.feats = value
                            "certainty" -> fs.misc = value
                        }
                    }
                res[fromTo] = fs
            }
        return res
    }

    private fun extractSentenceSpans(spans: NodeList): Array<Span> {
        return IntStream.range(0, spans.length)
            .mapToObj(spans::item)
            .filter { node -> node is Element && node.getElementsByTagName("f").item(0).textContent.equals("s") }
            .map { node ->
                Span(
                    Integer.parseInt((node as Element).getAttribute("from")),
                    Integer.parseInt(node.getAttribute("to"))
                )
            }
            .toArray { size -> arrayOfNulls(size) }
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

fun main(args: Array<String>) : Unit = exitProcess(CommandLine(KorapXml2Conllu()).execute(*args))

fun debug(args: Array<String>): Int {
    return(CommandLine(KorapXml2Conllu()).execute(*args))
}
