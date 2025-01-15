package de.ids_mannheim.korapxmltools

import de.ids_mannheim.korapxmltools.AnnotationToolBridgeFactory.Companion.parserFoundries
import de.ids_mannheim.korapxmltools.AnnotationToolBridgeFactory.Companion.taggerFoundries
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import org.xml.sax.SAXParseException
import picocli.CommandLine
import picocli.CommandLine.*
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.io.StringWriter
import java.lang.Integer.parseInt
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.logging.ConsoleHandler
import java.util.logging.Level
import java.util.logging.LogManager
import java.util.logging.Logger
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.stream.IntStream
import java.util.zip.ZipEntry

import java.util.zip.ZipFile
import java.util.zip.ZipOutputStream
import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.OutputKeys
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import kotlin.math.min
import kotlin.system.exitProcess

val ZIP_ENTRY_UNIX_MODE = parseInt("644", 8)

@Command(
    name = "KorapXml2Conllu",
    mixinStandardHelpOptions = true,
    version = ["KorapXml2Conllu 2.0-alpha-05"],
    description = ["Converts KorAP-XML <https://github.com/KorAP/KorAP-XML-Krill#about-korap-xml> base or " +
            "morpho zips to (annotated) CoNLL(-U) format with all information necessary for " +
            "reconstruction in comment lines."]
)

class KorapXml2Conllu : Callable<Int> {
    val COMPATIBILITY_MODE = System.getenv("COMPATIBILITY_MODE") != null

    @Spec lateinit var spec : Model.CommandSpec

    @Parameters(arity = "1..*", description = ["At least one zip file name"])
    var zipFileNames: Array<String>? = null

    @Option(
        names = ["-f", "--output-format"],
        description = ["Output format: ${ConlluOutputFormat.NAME}, ${Word2VecOutputFormat.NAME}, ${KorapXmlOutputFormat.NAME}",
            "conllu: CoNLL-U format",
            "korapxml, xml, zip: KorAP-XML format zip",
            "word2vec, w2v: Print text in LM training format: tokens separated by space, sentences separated by newlines",
        ],
        converter = [OutputFormatConverter::class]
    )
    var outputFormat: OutputFormat = OutputFormat.CONLLU
    class OutputFormatConverter : ITypeConverter<OutputFormat> {
        override fun convert(value: String?): OutputFormat {
            return when (value?.lowercase(Locale.getDefault())) {
                "conllu", "conll" -> OutputFormat.CONLLU
                "word2vec", "w2v" -> OutputFormat.WORD2VEC
                "korapxml", "korap", "xml", "zip" -> OutputFormat.KORAPXML
                else -> throw IllegalArgumentException("Unknown output format: `$value'. Use one of: ${OutputFormat.entries.joinToString(", ") { it.name }}")
            }
        }
    }

    @Option(
        names = ["--sigle-pattern", "-p"],
        paramLabel = "PATTERN",
        description = ["Extract only documents with sigle matching the pattern (regex)"]
    )
    var siglePattern: String? = null

    @Option(
        names = ["--extract-attributes-regex", "-e"],
        paramLabel = "REGEX",
        description = ["Extract additional attribute values from structure.xml and writes them as comment line in front of the first covered token.",
            "Example: -e '(posting/id|div/id)'"]
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
        description = ["Print text in LM training format: tokens separated by space, sentences separated by newline",
            "Deprecated: use -f word2vec"]
    )
    fun setWord2Vec(word2vec: Boolean) {
        if (word2vec) {
            outputFormat = OutputFormat.WORD2VEC
        }
    }

    @Option(
        names = ["--token-separator", "-s"],
        paramLabel = "STRING",
        defaultValue = "\n",
        description = ["Token separator. Default: new-line for CoNLL-U, space for word2vec format."]
    )
    var tokenSeparator: String = if (outputFormat == OutputFormat.WORD2VEC) " " else "\n"

    @Option(names = ["--offsets"], description = ["Not yet implemented: offsets"])
    var offsets: Boolean = false

    @Option(names = ["--comments", "-C"], description = ["Not yet implemented: comments"])
    var comments: Boolean = false

    @Option(
        names = ["--extract-metadata-regex", "-m"],
        paramLabel = "REGEX",
        description = ["Extract metadata regexes.\nExample: -m '<textSigle>([^<]+)' -m '<creatDate>([^<]+)'"]
    )
    var extractMetadataRegex: MutableList<String> = mutableListOf()

    @Option(
        names = ["--annotate-with", "-A"],
        paramLabel = "COMMAND",
        description = ["Pipe output through command"]
    )
    var annotateWith: String = ""

    @Option(
        names = ["--threads", "-T"],
        paramLabel = "THREADS",
        description = ["Maximum number of threads to use. Default: ${"$"}{DEFAULT-VALUE}"]
    )
    var maxThreads: Int = Runtime.getRuntime().availableProcessors() / 2
    fun setThreads(threads: Int) {
        if (threads < 1) {
            throw ParameterException(spec.commandLine(), String.format("Invalid value `%d' for option '--threads': must be at least 1", threads))
        }
        this.maxThreads = threads
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", threads.toString())
    }

    @Option(
        names = ["--overwrite", "-o"],
        description = ["Overwrite existing files"]
    )
    var overwrite: Boolean = false

    private var taggerName: String? = null
    private var taggerModel: String? = null
    @Option(
        names = ["--tag-with", "-t"],
        paramLabel = "TAGGER:MODEL",
        description = ["Specify a tagger and a model: ${taggerFoundries}:<path/to/model>."]
    )
    fun setTagWith(tagWith: String) {
        val pattern: Pattern = Pattern.compile("(${taggerFoundries}):(.+)")
        val matcher: Matcher = pattern.matcher(tagWith)
        if (!matcher.matches()) {
            throw ParameterException(spec.commandLine(),
                String.format("Invalid value `%s' for option '--tag-with': "+
                    "value does not match the expected pattern ${taggerFoundries}:<path/to/model>", tagWith))
        } else {
            taggerName = matcher.group(1)
            taggerModel = matcher.group(2)
            if (!File(taggerModel).exists()) {
                throw ParameterException(spec.commandLine(),
                    String.format("Invalid value for option '--tag-with':"+
                        "model file '%s' does not exist", taggerModel, taggerModel))
            }
        }
    }

    private var parserName: String? = null
    private var parserModel: String? = null
    @Option(
        names = ["--parse-with", "-P"],
        paramLabel = "parser:MODEL",
        description = ["Specify a parser and a model: ${parserFoundries}:<path/to/model>."]
    )
    fun setParseWith(parseWith: String) {
        val pattern: Pattern = Pattern.compile("(${parserFoundries}):(.+)")
        val matcher: Matcher = pattern.matcher(parseWith)
        if (!matcher.matches()) {
            throw ParameterException(spec.commandLine(),
                String.format("Invalid value `%s' for option '--parse-with': "+
                        "value does not match the expected pattern (${parserFoundries}):<path/to/model>", parseWith))
        } else {
            parserName = matcher.group(1)
            parserModel = matcher.group(2)
            if (!File(parserModel).exists()) {
                throw ParameterException(spec.commandLine(),
                    String.format("Invalid value for option '--parse-with':"+
                            "model file '%s' does not exist", parserModel, parserModel))
            }
        }
    }


    override fun call(): Int {
        val handler = ConsoleHandler()
        LogManager.getLogManager().reset()
        handler.formatter = ColoredFormatter()

        for (handler in LOGGER.handlers) {
            LOGGER.removeHandler(handler)
        }
        LOGGER.addHandler(handler)
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

    private var annotationWorkerPool : AnnotationWorkerPool? = null

    val texts: ConcurrentHashMap<String, NonBmpString> = ConcurrentHashMap()
    val sentences: ConcurrentHashMap<String, Array<Span>> = ConcurrentHashMap()
    val tokens: ConcurrentHashMap<String, Array<Span>> = ConcurrentHashMap()
    val morpho: ConcurrentHashMap<String, MutableMap<String, MorphoSpan>> = ConcurrentHashMap()
    val fnames: ConcurrentHashMap<String, String> = ConcurrentHashMap()
    val metadata: ConcurrentHashMap<String, Array<String>> = ConcurrentHashMap()
    val extraFeatures: ConcurrentHashMap<String, MutableMap<String, String>> = ConcurrentHashMap()
    var taggerToolBridges: ConcurrentHashMap<Long, TaggerToolBridge?> = ConcurrentHashMap()
    var parserToolBridges: ConcurrentHashMap<Long, ParserToolBridge?> = ConcurrentHashMap()

    var dbFactory: DocumentBuilderFactory? = null
    var dBuilder: DocumentBuilder? = null
    var byteArrayOutputStream: ByteArrayOutputStream? = null
    var morphoZipOutputStream: ZipOutputStream? = null

    fun String.hasCorrespondingBaseZip(): Boolean {
        if (!this.matches(Regex(".*\\.([^/.]+)\\.zip$"))) return false
        val baseZip = this.replace(Regex("\\.([^/.]+)\\.zip$"), ".zip")
        return File(baseZip).exists()
    }

    fun String.correspondingBaseZip(): String? {
        if (!this.matches(Regex(".*\\.([^/.]+)\\.zip$"))) return null
        val baseZip = this.replace(Regex("\\.([^/.]+)\\.zip$"), ".zip")
        return if (File(baseZip).exists()) baseZip else null
    }

    fun korapxml2conllu(args: Array<String>) {
        if (outputFormat == OutputFormat.KORAPXML && annotateWith.isNotEmpty()) {
            LOGGER.severe("Shell command annotation is not yet supported with output format $outputFormat")
            exitProcess(1)
        }
        Executors.newFixedThreadPool(maxThreads)

        if (annotateWith.isNotEmpty()) {
            annotationWorkerPool = AnnotationWorkerPool(annotateWith, maxThreads, LOGGER)
        }

        var zips: Array<String> = args

        if (maxThreads > 1) {
            LOGGER.info("Processing zip files in parallel with $maxThreads threads")
            Arrays.stream(zips).parallel().forEach { zipFilePath ->
                processZipFile((zipFilePath ?: "").toString(), getFoundryFromZipFileNames(zips))
            }
        } else {
            LOGGER.info("Processing zip files sequentially")
            Arrays.stream(zips).forEachOrdered { zipFilePath ->
                processZipFileSequentially((zipFilePath ?: "").toString(), getFoundryFromZipFileNames(zips))
            }
        }

        if (annotationWorkerPool != null) {
            LOGGER.info("closing worker pool")
            annotationWorkerPool?.close()
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

    private fun processZipFile(zipFilePath: String, foundry: String = "base") {
        LOGGER.info("Processing ${zipFilePath} in thread ${Thread.currentThread().id}")
        if (outputFormat == OutputFormat.KORAPXML && dbFactory == null) {
            dbFactory = DocumentBuilderFactory.newInstance()
            dBuilder = dbFactory!!.newDocumentBuilder()
            byteArrayOutputStream = ByteArrayOutputStream()
            morphoZipOutputStream = ZipOutputStream(byteArrayOutputStream!!)
        }
        if (zipFilePath.hasCorrespondingBaseZip()) {
            val zips = arrayOf(zipFilePath, zipFilePath.correspondingBaseZip()!!)
            Arrays.stream(zips).parallel().forEach { zip ->
                ZipFile(zip).use { zipFile ->
                    zipFile.stream().filter({ extractMetadataRegex.isNotEmpty() || !it.name.contains("header.xml") })
                        .parallel().forEach { zipEntry ->
                            processZipEntry(zipFile, foundry, zipEntry, true)
                        }
                }
            }
        } else {
            ZipFile(zipFilePath).use { zipFile ->
                zipFile.stream().filter({ extractMetadataRegex.isNotEmpty() || !it.name.contains("header.xml") })
                    .parallel().forEach { zipEntry ->
                        processZipEntry(zipFile, foundry, zipEntry, false)
                    }
            }
        }
        if (outputFormat == OutputFormat.KORAPXML) {
            morphoZipOutputStream!!.close()
            val outputMorphoZipFileName = zipFilePath.replace(Regex("\\.zip$"), ".".plus(getMorphoFoundry()).plus(".zip"))
            if (File(outputMorphoZipFileName).exists() && !overwrite) {
                LOGGER.severe("Output file $outputMorphoZipFileName already exists. Use --overwrite to overwrite.")
                exitProcess(1)
            }
            File(outputMorphoZipFileName).writeBytes(byteArrayOutputStream!!.toByteArray())
        }
    }

    private fun processZipFileSequentially(zipFilePath: String, foundry: String = "base") {
        LOGGER.info("Processing ${zipFilePath} in thread ${Thread.currentThread().id}")
        if (zipFilePath.hasCorrespondingBaseZip()) {
            val zips = arrayOf(zipFilePath, zipFilePath.correspondingBaseZip()!!)
            Arrays.stream(zips).parallel().forEach { zip ->
                ZipFile(zip).use { zipFile ->
                    zipFile.stream().filter({ extractMetadataRegex.isNotEmpty() || !it.name.contains("header.xml") })
                        .parallel().forEach { zipEntry ->
                            processZipEntry(zipFile, foundry, zipEntry, true)
                        }
                }
            }
        } else {
            ZipFile(zipFilePath).use { zipFile ->
                zipFile.stream().filter({ extractMetadataRegex.isNotEmpty() || !it.name.contains("header.xml") })
                    //.sorted({ o1, o2 -> o1.name.compareTo(o2.name) })
                    .forEachOrdered() { zipEntry ->
                        processZipEntry(zipFile, foundry, zipEntry, false)
                    }
            }
        }
    }

    fun processZipEntry(zipFile: ZipFile, _foundry: String, zipEntry: ZipEntry, passedWaitForMorpho: Boolean) {
        var foundry = _foundry
        var waitForMorpho = passedWaitForMorpho
        LOGGER.finer("Processing ${zipEntry.name} in thread ${Thread.currentThread().id}")
        if (taggerName != null && !taggerToolBridges.containsKey(Thread.currentThread().id)) {
            val tagger = AnnotationToolBridgeFactory.getAnnotationToolBridge(taggerName!!, taggerModel!!, LOGGER) as TaggerToolBridge?
            taggerToolBridges[Thread.currentThread().id] = tagger
            if (tagger != null) {
                foundry = tagger.foundry
            }

        }
        if (parserName != null && !parserToolBridges.containsKey(Thread.currentThread().id)) {
            val parser = AnnotationToolBridgeFactory.getAnnotationToolBridge(parserName!!, parserModel!!, LOGGER) as ParserToolBridge?
            parserToolBridges[Thread.currentThread().id] = parser
            if (parser != null) {
                foundry = "$foundry dependency:${parser.foundry}"
            }
        }

        try {
            if (zipEntry.name.matches(Regex(".*(data|tokens|structure|morpho)\\.xml$"))) {
                val inputStream: InputStream = zipFile.getInputStream(zipEntry)
                val dbFactory: DocumentBuilderFactory = DocumentBuilderFactory.newInstance()
                val dBuilder: DocumentBuilder = dbFactory.newDocumentBuilder()

                val doc: Document = try {
                    dBuilder.parse(InputSource(XMLCommentFilterReader(inputStream, "UTF-8")))
                } catch (e: SAXParseException) {
                    LOGGER.warning("Error parsing file: " + zipEntry.name + " " + e.message)
                    return
                }

                doc.documentElement.normalize()
                val docId: String = doc.documentElement.getAttribute("docid")
                if (siglePattern != null && !Regex(siglePattern!!).containsMatchIn(docId)) {
                    return
                }
                // LOGGER.info("Processing file: " + zipEntry.getName())
                val fileName = zipEntry.name.replace(Regex(".*?/([^/]+\\.xml)$"), "$1")
                when (fileName) {
                    "data.xml" -> {
                        val textsList: NodeList = doc.getElementsByTagName("text")
                        if (textsList.length > 0) {
                            texts[docId] = NonBmpString(textsList.item(0).textContent)
                        }
                    }

                    "structure.xml" -> {
                        val spans: NodeList = doc.getElementsByTagName("span")
                        if (extractAttributesRegex.isNotEmpty())
                            extraFeatures[docId] = extractMiscSpans(spans)
                        sentences[docId] = extractSentenceSpans(spans)

                    }

                    "tokens.xml" -> {
                        if (!fnames.contains(docId)) {
                            fnames[docId] = zipEntry.name
                        }
                        val tokenSpans: NodeList = doc.getElementsByTagName("span")
                        tokens[docId] = extractSpans(tokenSpans)
                    }

                    "morpho.xml" -> {
                        waitForMorpho = true
                        fnames[docId] = zipEntry.name
                        val fsSpans: NodeList = doc.getElementsByTagName("span")
                        morpho[docId] = extractMorphoSpans(fsSpans)
                        tokens[docId] = extractSpans(fsSpans)
                    }
                }

                if (texts[docId] != null && sentences[docId] != null && tokens[docId] != null
                    && (!waitForMorpho || morpho[docId] != null)
                    && (extractMetadataRegex.isEmpty() || metadata[docId] != null)
                ) {
                    processText(docId, foundry)
                }
            } else if (extractMetadataRegex.isNotEmpty() && zipEntry.name.matches(Regex(".*/header\\.xml$"))) {
                //LOGGER.info("Processing header file: " + zipEntry.name)
                val text = zipFile.getInputStream(zipEntry).bufferedReader().use { it.readText() }
                val docId =
                    Regex("<textSigle>([^<]+)</textSigle>").find(text)?.destructured?.component1()
                        ?.replace(Regex("/"), "_")
                LOGGER.info("Processing header file: " + zipEntry.name + " docId: " + docId)
                val meta = ArrayList<String>()
                extractMetadataRegex.forEach { regex ->
                    val match = Regex(regex).find(text)
                    if (match != null) {
                        meta.add(match.destructured.component1())
                    }
                }
                if (meta.isNotEmpty() && docId != null) {
                    metadata[docId] = meta.toTypedArray()
                    if (texts[docId] != null && sentences[docId] != null && tokens[docId] != null
                        && (!waitForMorpho || morpho[docId] != null)
                    ) {
                        processText(docId, foundry)
                    }
                }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    private fun processText(
        docId: String,
        foundry: String,
    ) {
        var morphoFoundry = getMorphoFoundry()
        val output =
        if (outputFormat == OutputFormat.WORD2VEC) {
            lmTrainingOutput(docId)
        } else {
            if (taggerToolBridges[Thread.currentThread().id] != null) {
                morpho[docId] = taggerToolBridges[Thread.currentThread().id]!!.tagText(
                    tokens[docId]!!,
                    sentences[docId],
                    texts[docId]!!
                )
                if (parserToolBridges[Thread.currentThread().id] != null) {
                    morpho[docId] = parserToolBridges[Thread.currentThread().id]!!.parseText(
                        tokens[docId]!!,
                        morpho[docId],
                        sentences[docId],
                        texts[docId]!!
                    )
                }
            }
            if (outputFormat == OutputFormat.KORAPXML && annotationWorkerPool == null) {
                korapXmlOutput(getMorphoFoundry(), docId)
            } else {
                conlluOutput(foundry, docId)
            }
        }

        if (annotationWorkerPool != null) {
            annotationWorkerPool?.pushToQueue(output.append("\n# eot\n").toString())
        } else if (outputFormat != OutputFormat.KORAPXML) {
            synchronized(System.out) {
                println(output.toString())
            }
        } else {
            korapXmlOutput(foundry, docId)
        }


        arrayOf(tokens, texts, sentences, morpho, fnames, metadata, extraFeatures).forEach { map ->
            map.remove(docId)
        }

        if (outputFormat == OutputFormat.KORAPXML) {
            val entryPath = docId.replace(Regex("[_.]"), "/").plus("/$morphoFoundry/").plus("morpho.xml")
            val zipEntry = ZipEntry(entryPath)
            // val zipEntry = org.apache.tools.zip.ZipEntry(entryPath)
            // zipEntry.unixMode = 65535
            synchronized(morphoZipOutputStream!!) {
                morphoZipOutputStream!!.putNextEntry(zipEntry)
                morphoZipOutputStream!!.write(output.toString().toByteArray())
                morphoZipOutputStream!!.closeEntry()
            }
            output.clear()
        }
    }

    private fun getMorphoFoundry() = taggerToolBridges[Thread.currentThread().id]?.foundry ?: "base"

    private fun korapXmlOutput(foundry: String, docId: String): StringBuilder {
        val doc: Document = dBuilder!!.newDocument()

        // Root element
        val layer = doc.createElement("layer")
        layer.setAttribute("xmlns", "http://ids-mannheim.de/ns/KorAP")
        layer.setAttribute("version", "KorAP-0.4")
        layer.setAttribute("docid", docId)
        doc.appendChild(layer)

        val spanList = doc.createElement("spanList")
        layer.appendChild(spanList)

        var i = 0
        morpho[docId]?.forEach { (spanString, mfs) ->
            i++
            val offsets = spanString.split("-")
            val spanNode = doc.createElement("span")
            spanNode.setAttribute("id", "t_$i")
            spanNode.setAttribute("from", offsets[0])
            spanNode.setAttribute("to", offsets[1])

            // fs element
            val fs = doc.createElement("fs")
            fs.setAttribute("type", "lex")
            fs.setAttribute("xmlns", "http://www.tei-c.org/ns/1.0")
            spanNode.appendChild(fs)
            val f = doc.createElement("f")
            f.setAttribute("name", "lex")
            fs.appendChild(f)

            // Inner fs element
            val innerFs = doc.createElement("fs")
            f.appendChild(innerFs)

            if (mfs.lemma != "_") {
                val innerF = doc.createElement("f")
                innerF.setAttribute("name", "lemma")
                innerF.textContent = mfs.lemma
                innerFs.appendChild(innerF)
            }
            if (mfs.upos != "_") {
                val innerF = doc.createElement("f")
                innerF.setAttribute("name", "upos")
                innerF.textContent = mfs.upos
                innerFs.appendChild(innerF)
            }
            if (mfs.xpos != "_") {
                val innerF = doc.createElement("f")
                innerF.setAttribute("name", "pos")
                innerF.textContent = mfs.xpos
                innerFs.appendChild(innerF)
            }
            if (mfs.feats != "_") {
                val innerF = doc.createElement("f")
                innerF.setAttribute("name", "msd")
                innerF.textContent = mfs.feats
                innerFs.appendChild(innerF)
            }
            if (mfs.misc != "_" && mfs.misc!!.matches(Regex("^[0-9.]+$"))) {
                val innerF = doc.createElement("f")
                innerF.setAttribute("name", "certainty")
                innerF.textContent = mfs.misc
                innerFs.appendChild(innerF)
            }

            spanList.appendChild(spanNode)
        }
        val transformerFactory = TransformerFactory.newInstance()
        val transformer = transformerFactory.newTransformer()
        transformer.setOutputProperty(OutputKeys.INDENT, "yes")
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no")
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "1")
        val domSource = DOMSource(doc)
        val streamResult = StreamResult(StringWriter())
        transformer.transform(domSource, streamResult)

        return StringBuilder(streamResult.writer.toString())

    }

    private fun conlluOutput(foundry: String, docId: String): StringBuilder {
        var token_index = 0
        var real_token_index = 0
        var sentence_index = 0
        val output: StringBuilder
        output =
            StringBuilder("# foundry = $foundry\n# filename = ${fnames[docId]}\n# text_id = $docId\n").append(
                tokenOffsetsInSentence(
                    sentences, docId, sentence_index, real_token_index, tokens
                )
            )
        if (extractMetadataRegex.isNotEmpty()) {
            output.append(metadata[docId]?.joinToString("\t", prefix = "# metadata=", postfix = "\n") ?: "")
        }
        var previousSpanStart = 0
        tokens[docId]?.forEach { span ->
            token_index++
            if (sentence_index >= sentences[docId]!!.size || span.from >= sentences[docId]!![sentence_index].to) {
                output.append("\n")
                sentence_index++
                token_index = 1
                output.append(
                    tokenOffsetsInSentence(
                        sentences, docId, sentence_index, real_token_index, tokens
                    )
                )
            }
            if (extractAttributesRegex.isNotEmpty() && extraFeatures[docId] != null) {
                for (i in previousSpanStart until span.from + 1) {
                    if (extraFeatures[docId]?.containsKey("$i") == true) {
                        output.append(extraFeatures[docId]!!["$i"])
                        extraFeatures[docId]!!.remove("$i")
                    }
                }
                previousSpanStart = span.from + 1
            }
            if (morpho[docId]?.containsKey("${span.from}-${span.to}") == true) {
                val mfs = morpho[docId]!!["${span.from}-${span.to}"]
                if (span.to > texts[docId]!!.length) {
                    span.to = texts[docId]!!.length
                    LOGGER.warning(
                        "Offset error: could not retrieve token at ${span.from}-${span.to} – ending with: ${
                            texts[docId]!!.substring(
                                span.from,
                                span.to
                            )
                        }"
                    )
                }
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
        return output
    }

    private fun lmTrainingOutput(docId: String): StringBuilder {
        var token_index = 0
        var real_token_index = 0
        var sentence_index = 0
        val output: StringBuilder
        output = StringBuilder()
        if (extractMetadataRegex.isNotEmpty()) {
            output.append(metadata[docId]?.joinToString("\t", postfix = "\t") ?: "")
        }
        if (texts[docId] == null) {
            return output
        }
        tokens[docId]?.forEach { span ->
            token_index++
            if (sentences[docId] != null && (sentence_index >= sentences[docId]!!.size || span.from >= sentences[docId]!![sentence_index].to)) {
                if (output.isNotEmpty()) {
                    output.setCharAt(output.length - 1, '\n')
                } else {
                    output.append("\n")
                }
                if (extractMetadataRegex.isNotEmpty() && real_token_index < tokens[docId]!!.size - 1) {
                    output.append(metadata[docId]?.joinToString("\t", postfix = "\t") ?: "")
                }
                sentence_index++
            }
            output.append(texts[docId]!!.substring(span.from, span.to), " ")
            real_token_index++
        }
        if (output.isNotEmpty()) {
            output.deleteCharAt(output.length - 1)
        }
        return output
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
        val myUpos = if (COMPATIBILITY_MODE && upos == "_") xpos else upos
        return when (columns) {
            1 -> ("$token\n")
            10 -> ("$token_index\t$token\t$lemma\t$myUpos\t$xpos\t$feats\t$head\t$deprel\t$deps\t$misc$tokenSeparator")
            else -> arrayOf(token_index, token, lemma, myUpos, xpos, feats, head, deprel, deps, misc).slice(0..<min(columns, 10))
                .joinToString("\t", postfix = tokenSeparator)
        }
    }

    private fun tokenOffsetsInSentence(
        sentences: ConcurrentHashMap<String, Array<Span>>,
        docId: String,
        sentence_index: Int,
        token_index: Int,
        tokens: ConcurrentHashMap<String, Array<Span>>
    ): String {
        if (sentences[docId] == null || sentences[docId]!!.size <= sentence_index) {
            return ""
        }
        val sentenceEndOffset = sentences[docId]!![sentence_index].to
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
        val UNKNOWN = Regex("(UNKNOWN|<unknown>)")
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
                            "lemma" -> if(fs.lemma == "_") fs.lemma = value.replace(UNKNOWN, "--")
                            "upos" -> fs.upos = value
                            "xpos", "ctag", "pos" -> if(fs.xpos == "_") fs.xpos = value.replace(UNKNOWN, "--")
                            "feats", "msd" -> if(fs.feats == "_" ) fs.feats = value
                            "type" -> if(fs.feats == "_") fs.feats = feature.getElementsByTagName("symbol").item(0).attributes.getNamedItem("value").textContent.trim()
                            // "subtype" -> if(fs.feats == "_") fs.feats += ":" + feature.getElementsByTagName("symbol").item(0).attributes.getNamedItem("value").textContent
                            "certainty" -> if(fs.misc == "_") fs.misc = value
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

    /*
     <span id="s15" from="370" to="394" l="5">
      <fs type="struct" xmlns="http://www.tei-c.org/ns/1.0">
        <f name="name">posting</f>
        <f name="attr">
          <fs type="attr">
            <f name="id">i.10894_1_3</f>
            <f name="indentLevel">0</f>
            <f name="who">WU00000000</f>
          </fs>
        </f>
      </fs>
    </span>

     */
    private fun extractMiscSpans(spans: NodeList): MutableMap<String, String> {
        val miscLocal: MutableMap<String, String> = HashMap()

        IntStream.range(0, spans.length).mapToObj(spans::item)
            .filter { node ->
                node is Element
                        && node.getElementsByTagName("f").length > 1
                        && (node.getElementsByTagName("f").item(0) as Element).getAttribute("name").equals("name")
                        && (node.getElementsByTagName("f").item(1) as Element).getAttribute("name").equals("attr")
            }
            .forEach { node ->
                if (node == null) return@forEach
                val elementName = (node as Element).getElementsByTagName("f").item(0).textContent.trim()
                val from = node.getAttribute("from")
                val attributes = (node.getElementsByTagName("f").item(1) as Element).getElementsByTagName("f")
                val res = StringBuilder()
                IntStream.range(0, attributes.length).mapToObj(attributes::item).forEach { attr ->
                    val attrName = "$elementName/${(attr as Element).getAttribute("name")}"
                    if (attrName.matches(Regex(extractAttributesRegex))) {
                         res.append("# $attrName = ${attr.textContent}\n")
                        //LOGGER.info("" + from + ": $attrName = " + attr.textContent)
                    }

                }
                if (res.isNotEmpty()) {
                    if (miscLocal.containsKey(from)) {
                        // LOGGER.info("ADDING TO $from: ${miscLocal[from]}")
                        miscLocal[from] += res.toString()
                    } else {
                        miscLocal[from] = res.toString()
                    }
                }
            }
        return miscLocal
    }


    class Span(var from: Int, var to: Int)

    class MorphoSpan(
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

enum class OutputFormat {
    CONLLU, WORD2VEC, KORAPXML
}

object ConlluOutputFormat {
    const val NAME = "conllu"
}

object Word2VecOutputFormat {
    const val NAME = "word2vec"
}

object KorapXmlOutputFormat {
    const val NAME = "korapxml"
}


