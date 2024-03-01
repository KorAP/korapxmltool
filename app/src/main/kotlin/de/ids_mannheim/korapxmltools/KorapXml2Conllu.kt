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
import java.io.InputStreamReader
import java.util.logging.Logger

class KorapXml2Conllu {
    private val LOGGER: Logger = Logger.getLogger(KorapXml2Conllu::class.java.name)

    fun main(args: Array<String?>?) {
        val executor: ExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())
        val texts: ConcurrentHashMap<String, String> = ConcurrentHashMap()
        val sentences: ConcurrentHashMap<String, Array<Span>> = ConcurrentHashMap()
        val tokens: ConcurrentHashMap<String, Array<Span>> = ConcurrentHashMap()
        val morpho: ConcurrentHashMap<String, MutableMap<String, MorphoSpan>> = ConcurrentHashMap()
        val fnames: ConcurrentHashMap<String, String> = ConcurrentHashMap()

        Arrays.stream(args).forEach { zipFilePath ->
            executor.submit {
                processZipFile(
                    zipFilePath ?: "",
                    texts,
                    sentences,
                    tokens,
                    fnames,
                    morpho,
                    args!!.size > 1
                )
            }
        }

        executor.shutdown()
        while (!executor.isTerminated) {
            // Wait for all tasks to finish
        }

        // Further processing as needed
    }

    private fun processZipFile(
        zipFilePath: String,
        texts: ConcurrentHashMap<String, String>,
        sentences: ConcurrentHashMap<String, Array<Span>>,
        tokens: ConcurrentHashMap<String, Array<Span>>,
        fname: ConcurrentHashMap<String, String>,
        morpho: ConcurrentHashMap<String, MutableMap<String, MorphoSpan>>,
        waitForMorpho: Boolean = false
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
                                    extractMorphoSpans(fsSpans, docId, morpho)
                                }
                            }
                            if (texts[docId] != null && sentences[docId] != null && tokens[docId] != null
                                && (!waitForMorpho || morpho[docId] != null)
                            ) {
                                synchronized(System.out) {
                                    println("# foundry = base")
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
        while (i < tokens[docId]!!.size && tokens[docId]!![i].to <= sentenceEndOffset) {
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
        fsSpans: NodeList,
        docId: String,
        morpho: ConcurrentHashMap<String, MutableMap<String, MorphoSpan>>
    ) {
        IntStream.range(0, fsSpans.length)
            .mapToObj(fsSpans::item)
            .forEach { node ->
                val features = (node as Element).getElementsByTagName("f")
                var fs = MorphoSpan()
                val fromTo = node.getAttribute("from") + "-" + node.getAttribute("to")
                IntStream.range(0, features.length).mapToObj(features::item)
                    .forEach { feature ->
                        val attr = (feature as Element).getAttribute("name")
                        val value = feature.textContent
                        when (attr) {
                            "lemma" -> fs.lemma = value
                            "upos" -> fs.upos = value
                            "xpos" -> fs.xpos = value
                            "certainty" -> fs.misc = value
                            "ctag", "pos" -> fs.xpos = value
                        }
                    }
                if (morpho[docId] == null) {
                    morpho[docId] = mutableMapOf()
                }
                morpho[docId]!![fromTo] = fs
            }
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


fun main(args: Array<String?>?) {
    System.setProperty("file.encoding", "UTF-8")
    KorapXml2Conllu().main(args)
}

