package de.ids_mannheim.korapxmltools

import de.ids_mannheim.korapxmltools.AnnotationToolBridgeFactory.Companion.parserFoundries
import de.ids_mannheim.korapxmltools.AnnotationToolBridgeFactory.Companion.taggerFoundries
import org.apache.commons.compress.archivers.zip.Zip64Mode
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import org.xml.sax.SAXParseException
import picocli.CommandLine
import picocli.CommandLine.*
import java.io.File
import java.io.FileOutputStream
import java.io.InputStream
import java.io.StringWriter
import java.lang.Integer.parseInt
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.ConsoleHandler
import java.util.logging.Level
import java.util.logging.LogManager
import java.util.logging.Logger
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.stream.IntStream
import java.util.zip.ZipEntry

import java.util.zip.ZipFile
import me.tongfei.progressbar.ProgressBar
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
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
    name = "KorapXmlTool",
    mixinStandardHelpOptions = true,
    version = ["KorapXmlTool 2.01"],
    description = ["Converts KorAP-XML <https://github.com/KorAP/KorAP-XML-Krill#about-korap-xml> base or " +
            "morpho zips to (annotated) CoNLL(-U) format with all information necessary for " +
            "reconstruction in comment lines."]
)

class KorapXmlTool : Callable<Int> {
    val COMPATIBILITY_MODE = System.getenv("COMPATIBILITY_MODE") != null

    @Spec lateinit var spec : Model.CommandSpec

    @Parameters(arity = "1..*", description = ["At least one zip file name"])
    var zipFileNames: Array<String>? = null

    @Option(
        names = ["-f", "--output-format"],
        description = ["Output format: ${ConlluOutputFormat.NAME}, ${Word2VecOutputFormat.NAME}, ${KorapXmlOutputFormat.NAME}, ${NowOutputFormat.NAME}",
            "conllu: CoNLL-U format",
            "korapxml, xml, zip: KorAP-XML format zip",
            "word2vec, w2v: Print text in LM training format: tokens separated by space, sentences separated by newlines",
            "now, NOW: NOW corpus export format: w2v-like format with <p> tags for sentence ends and @@<text-sigle> prefix",
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
                "now", "NOW" -> OutputFormat.NOW
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
        names = ["--exclude-zip-glob"],
        paramLabel = "GLOB",
        description = [
            "Exclude zip files whose basename matches the glob (e.g., 'w?d24.tree_tagger.zip').",
            "May be repeated. Applied to basenames, not full paths."
        ]
    )
    var excludeZipGlobs: MutableList<String> = mutableListOf()

    @Option(
        names = ["--token-separator", "-s"],
        paramLabel = "STRING",
        defaultValue = "\n",
        description = ["Token separator. Default: new-line for CoNLL-U, space for word2vec format."]
    )
    var tokenSeparator: String = if (outputFormat == OutputFormat.WORD2VEC || outputFormat == OutputFormat.NOW) " " else "\n"

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
        names = ["--zip-parallelism"],
        paramLabel = "N",
        description = ["Maximum number of zip files to process concurrently. Defaults to --threads."]
    )
    var zipParallelism: Int? = null

    @Option(
        names = ["--sequential"],
        description = [
            "Process entries inside each zip sequentially; zips processed in parallel (only for word2vec/now)."
        ]
    )
    var sequentialInZip: Boolean = false

    @Option(
        names = ["--overwrite", "-o"],
        description = ["Overwrite existing files"]
    )
    var overwrite: Boolean = false

    @Option(
        names = ["--mem-stats-interval"],
        paramLabel = "N",
        description = ["Log memory and cache statistics every N processed documents (0 disables; default: 0)"]
    )
    var memStatsInterval: Int = 0

    @Option(
        names = ["--lemma"],
        description = ["In word2vec/now output modes, output lemmas instead of surface tokens when lemma annotations are available (requires corresponding morpho annotation XML)"]
    )
    var useLemma: Boolean = false

    @Option(
        names = ["--lemma-only"],
        description = [
            "Do not load texts from data.xml and output only lemmas (requires morpho.xml).",
            "Only valid with -f word2vec or -f now; implies --lemma."
        ]
    )
    var lemmaOnly: Boolean = false

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

        if (lemmaOnly) {
            useLemma = true
            if (outputFormat != OutputFormat.WORD2VEC && outputFormat != OutputFormat.NOW) {
                throw ParameterException(spec.commandLine(), "--lemma-only is supported only with -f word2vec or -f now")
            }
        }

        LOGGER.info("Processing zip files: " + zipFileNames!!.joinToString(", "))

        korapxml2conllu(zipFileNames!!)
        return 0
    }

    private val LOGGER: Logger = Logger.getLogger(KorapXmlTool::class.java.name)

    private var annotationWorkerPool : AnnotationWorkerPool? = null
    // Shared executor for entry-level parallelism across all zips
    private var entryExecutor: java.util.concurrent.ExecutorService? = null

    val texts: ConcurrentHashMap<String, NonBmpString> = ConcurrentHashMap()
    val sentences: ConcurrentHashMap<String, Array<Span>> = ConcurrentHashMap()
    val tokens: ConcurrentHashMap<String, Array<Span>> = ConcurrentHashMap()
    val morpho: ConcurrentHashMap<String, MutableMap<String, MorphoSpan>> = ConcurrentHashMap()
    val fnames: ConcurrentHashMap<String, String> = ConcurrentHashMap()
    val metadata: ConcurrentHashMap<String, Array<String>> = ConcurrentHashMap()
    val extraFeatures: ConcurrentHashMap<String, MutableMap<String, String>> = ConcurrentHashMap()
    private val processedDocs = java.util.concurrent.atomic.AtomicInteger(0)
    private val docsSentToAnnotation = java.util.concurrent.atomic.AtomicInteger(0)
    private val docsWrittenToZip = java.util.concurrent.atomic.AtomicInteger(0)
    private val totalDocsInInput = java.util.concurrent.atomic.AtomicInteger(0) // Track total documents for progress
    private val annotationStartTime = java.util.concurrent.atomic.AtomicLong(0) // Track when annotation started
    private var progressBar: ProgressBar? = null
    var taggerToolBridges: ConcurrentHashMap<Long, TaggerToolBridge?> = ConcurrentHashMap()
    var parserToolBridges: ConcurrentHashMap<Long, ParserToolBridge?> = ConcurrentHashMap()

    // Zip progress tracking for logging (zipNumber/zipTotal)
    private val zipOrdinals: ConcurrentHashMap<String, Int> = ConcurrentHashMap()
    private var totalZips: Int = 0
    private val zipSizes: ConcurrentHashMap<String, Long> = ConcurrentHashMap()
    private val processedZipBytes: AtomicLong = AtomicLong(0)
    private var totalZipBytes: Long = 0
    private var startTimeMillis: Long = 0

    var dbFactory: DocumentBuilderFactory? = null
    var dBuilder: DocumentBuilder? = null
    var morphoZipOutputStream: ZipArchiveOutputStream? = null

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
        // Initialize shared entry executor (used inside each zip)
        entryExecutor = Executors.newFixedThreadPool(maxThreads)

        if (annotateWith.isNotEmpty()) {
            // Initialize ZIP output stream BEFORE creating worker pool, if needed
            if (outputFormat == OutputFormat.KORAPXML) {
                // Determine output filename
                val inputZipPath = args[0] // First ZIP file
                var targetFoundry = "base"
                when {
                    annotateWith.contains("spacy") -> targetFoundry = "spacy"
                    annotateWith.contains("stanza") -> targetFoundry = "stanza"
                    annotateWith.contains("udpipe") -> targetFoundry = "udpipe"
                    annotateWith.contains("tree") -> targetFoundry = "tree_tagger"
                    annotateWith.contains("marmot") -> targetFoundry = "marmot"
                    annotateWith.contains("opennlp") -> targetFoundry = "opennlp"
                    annotateWith.contains("corenlp") -> targetFoundry = "corenlp"
                    else -> targetFoundry = "annotated"
                }

                val outputMorphoZipFileName = inputZipPath.replace(Regex("\\.zip$"), ".".plus(targetFoundry).plus(".zip"))
                LOGGER.info("Initializing output ZIP: $outputMorphoZipFileName (from input: $inputZipPath, foundry: $targetFoundry)")

                if (File(outputMorphoZipFileName).exists() && !overwrite) {
                    LOGGER.severe("Output file $outputMorphoZipFileName already exists. Use --overwrite to overwrite.")
                    exitProcess(1)
                }

                // Delete old file if it exists
                if (File(outputMorphoZipFileName).exists()) {
                    LOGGER.info("Deleting existing file: $outputMorphoZipFileName")
                    File(outputMorphoZipFileName).delete()
                }

                dbFactory = DocumentBuilderFactory.newInstance()
                dBuilder = dbFactory!!.newDocumentBuilder()
                val fileOutputStream = FileOutputStream(outputMorphoZipFileName)
                morphoZipOutputStream = ZipArchiveOutputStream(fileOutputStream).apply {
                    setUseZip64(Zip64Mode.Always)
                }
                LOGGER.info("Initialized morphoZipOutputStream for external annotation to: $outputMorphoZipFileName")
            }

            if (outputFormat == OutputFormat.KORAPXML) {
                // For ZIP output with external annotation, we need a custom handler
                annotationWorkerPool = AnnotationWorkerPool(annotateWith, maxThreads, LOGGER) { annotatedConllu, task ->
                    parseAndWriteAnnotatedConllu(annotatedConllu, task)
                }
            } else {
                annotationWorkerPool = AnnotationWorkerPool(annotateWith, maxThreads, LOGGER, null)
            }
        }

        var zips: Array<String> = args
        if (excludeZipGlobs.isNotEmpty()) {
            val before = zips.size
            val patterns = excludeZipGlobs.map { globToRegex(it) }
            zips = zips.filter { zipPath ->
                val base = File(zipPath).name
                patterns.none { rx -> rx.matches(base) }
            }.toTypedArray()
            val excluded = before - zips.size
            if (excluded > 0) {
                LOGGER.info("Excluded $excluded of $before zip(s) by glob(s): ${excludeZipGlobs.joinToString(", ")}")
            }
        }
        // Initialize zip progress tracking and sizes
        startTimeMillis = System.currentTimeMillis()
        processedZipBytes.set(0)
        totalZips = zips.size
        zipOrdinals.clear()
        zipSizes.clear()
        zips.forEach { zip -> zipSizes[zip] = try { File(zip).length() } catch (_: Exception) { 0L } }
        totalZipBytes = zipSizes.values.sum()
        // In lemma-only mode, process largest zips first
        if (lemmaOnly) {
            zips = zips.sortedByDescending { zipSizes[it] ?: 0L }.toTypedArray()
        }
        zips.forEachIndexed { index, zip -> zipOrdinals[zip] = index + 1 }

        // Log zip order with sizes so the user can verify sorting
        val totalHuman = humanBytes(totalZipBytes)
        LOGGER.info("Zip processing order (${zips.size} file(s), total ${totalHuman}):")
        zips.forEachIndexed { idx, zip ->
            val size = zipSizes[zip] ?: 0L
            LOGGER.info(String.format(Locale.ROOT, "%d/%d: %s (%s)", idx + 1, zips.size, zip, humanBytes(size)))
        }

        if (sequentialInZip) {
            if (outputFormat != OutputFormat.WORD2VEC && outputFormat != OutputFormat.NOW) {
                throw ParameterException(spec.commandLine(), "--sequential is supported only with -f word2vec or -f now")
            }
        }

        if (maxThreads > 1) {
            val foundry = getFoundryFromZipFileNames(zips)
            val parallelism = (zipParallelism ?: maxThreads).coerceAtLeast(1)
            LOGGER.info("Processing zips with ordered queue; parallelism=$parallelism; entries ${if (sequentialInZip) "sequential" else "parallel"}")
            processZipsWithQueue(zips, foundry, parallelism)
        } else {
            LOGGER.info("Processing zip files sequentially")
            Arrays.stream(zips).forEachOrdered { zipFilePath ->
                processZipFileSequentially((zipFilePath ?: "").toString(), getFoundryFromZipFileNames(zips))
            }
        }

        // Shutdown entry executor BEFORE closing worker pool to ensure no more tasks enqueue output after EOF
        entryExecutor?.shutdown()
        try {
            if (entryExecutor != null) {
                val terminated = entryExecutor!!.awaitTermination(7, java.util.concurrent.TimeUnit.DAYS)
                if (!terminated) {
                    LOGGER.warning("Entry executor did not terminate within timeout; proceeding to close worker pool.")
                }
            }
        } catch (ie: InterruptedException) {
            Thread.currentThread().interrupt()
            LOGGER.warning("Interrupted while awaiting entry executor termination; proceeding to close worker pool.")
        }

        if (annotationWorkerPool != null) {
            LOGGER.info("closing worker pool")
            LOGGER.info("Documents sent to annotation: ${docsSentToAnnotation.get()}")
            annotationWorkerPool?.close()
            LOGGER.info("Documents written to ZIP: ${docsWrittenToZip.get()}")

            // Close the ZIP file after worker pool is done (if using external annotation with ZIP output)
            if (outputFormat == OutputFormat.KORAPXML && morphoZipOutputStream != null) {
                try {
                    morphoZipOutputStream!!.flush()
                    morphoZipOutputStream!!.close()
                    LOGGER.info("Closed output ZIP file after annotation processing")
                } catch (e: Exception) {
                    LOGGER.severe("ERROR closing ZIP file: ${e.message}")
                    e.printStackTrace()
                }
            }

            // Close progress bar
            progressBar?.close()

            // Check if all documents were written
            val sent = docsSentToAnnotation.get()
            val written = docsWrittenToZip.get()
            if (sent != written) {
                LOGGER.warning("Document count mismatch! Sent to annotation: $sent, Written to ZIP: $written (missing: ${sent - written})")
            }
        }
        // Shutdown entry executor
        entryExecutor?.shutdown()
    }

    private fun processZipsWithQueue(zips: Array<String>, foundry: String, parallelism: Int) {
        val queue: java.util.concurrent.BlockingQueue<String> = java.util.concurrent.LinkedBlockingQueue()
        zips.forEach { queue.put(it) }
        val executor = Executors.newFixedThreadPool(parallelism)
        val active = java.util.concurrent.atomic.AtomicInteger(0)
        repeat(parallelism) {
            executor.submit {
                active.incrementAndGet()
                try {
                    while (true) {
                        val zipPath = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
                        if (zipPath == null) {
                            if (queue.isEmpty()) break else continue
                        }
                        if (sequentialInZip) {
                            processZipFileSequentially(zipPath, foundry)
                        } else {
                            processZipFile(zipPath, foundry)
                        }
                    }
                } finally {
                    active.decrementAndGet()
                }
            }
        }
        executor.shutdown()
        try {
            executor.awaitTermination(7, java.util.concurrent.TimeUnit.DAYS)
        } catch (ie: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    // Convert a shell-like glob to a Regex: '*' -> ".*", '?' -> '.', anchored full match
    private fun globToRegex(glob: String): Regex {
        val sb = StringBuilder("^")
        glob.forEach { ch ->
            when (ch) {
                '*' -> sb.append(".*")
                '?' -> sb.append('.')
                '.', '(', ')', '+', '|', '^', '$', '@', '%', '{', '}', '[', ']', '\\' -> sb.append('\\').append(ch)
                else -> sb.append(ch)
            }
        }
        sb.append('$')
        return Regex(sb.toString())
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
        val ord = zipOrdinals[zipFilePath] ?: 0
        val size = zipSizes[zipFilePath] ?: 0L
        LOGGER.info("Processing zip ${if (ord>0) ord else "?"}/$totalZips: ${zipFilePath} (${humanBytes(size)}) in thread ${Thread.currentThread().threadId()}")
        LOGGER.info("Foundry: $foundry $dbFactory")
        if (outputFormat == OutputFormat.KORAPXML && dbFactory == null) {
            var targetFoundry = "base"
            if (taggerName != null) {
                val tagger = AnnotationToolBridgeFactory.getAnnotationToolBridge(taggerName!!, taggerModel!!, LOGGER) as TaggerToolBridge?
                if (tagger != null) {
                    targetFoundry = tagger.foundry
                }
            } else if (parserName != null) {
                targetFoundry = parserName!!
            } else if (annotateWith.isNotEmpty()) {
                // Try to detect foundry from external annotation command
                when {
                    annotateWith.contains("spacy") -> targetFoundry = "spacy"
                    annotateWith.contains("stanza") -> targetFoundry = "stanza"
                    annotateWith.contains("udpipe") -> targetFoundry = "udpipe"
                    annotateWith.contains("tree") -> targetFoundry = "tree_tagger"
                    annotateWith.contains("marmot") -> targetFoundry = "marmot"
                    annotateWith.contains("opennlp") -> targetFoundry = "opennlp"
                    annotateWith.contains("corenlp") -> targetFoundry = "corenlp"
                    else -> targetFoundry = "annotated"
                }
                LOGGER.info("Detected foundry '$targetFoundry' from annotation command: $annotateWith")
            }
            dbFactory = DocumentBuilderFactory.newInstance()
            dBuilder = dbFactory!!.newDocumentBuilder()
            val outputMorphoZipFileName =
                if (parserName != null)
                    zipFilePath.replace(Regex("(\\.(opennlp|marmot|tree_tagger|corenlp|spacy))?\\.zip$"), ".".plus(parserName).plus(".zip"))
                else
                    zipFilePath.replace(Regex("\\.zip$"), ".".plus(targetFoundry).plus(".zip"))
            LOGGER.info("Output ZIP file: $outputMorphoZipFileName")
            if (File(outputMorphoZipFileName).exists() && !overwrite) {
                LOGGER.severe("Output file $outputMorphoZipFileName already exists. Use --overwrite to overwrite.")
                exitProcess(1)
            }
            val fileOutputStream = FileOutputStream(outputMorphoZipFileName)
            morphoZipOutputStream = ZipArchiveOutputStream(fileOutputStream).apply {
                setUseZip64(Zip64Mode.Always)
            }
            LOGGER.info("Initialized morphoZipOutputStream for $outputMorphoZipFileName")
        } else {
            LOGGER.info("Skipping ZIP initialization: dbFactory=${dbFactory != null}, outputFormat=$outputFormat")
        }
        if (zipFilePath.hasCorrespondingBaseZip()) {
            val relatedZips = arrayOf(zipFilePath, zipFilePath.correspondingBaseZip()!!)
            // Process related zips one after another to keep the ZipFile lifetime strictly bounded
            relatedZips.forEach { zip ->
                ZipFile(zip).use { zipFile ->
                    processZipEntriesWithPool(zipFile, foundry, true)
                }
            }
        } else {
            ZipFile(zipFilePath).use { zipFile ->
                processZipEntriesWithPool(zipFile, foundry, false)
            }
        }
        // Don't close the ZIP here if using external annotation - it will be closed after worker pool finishes
        if (outputFormat == OutputFormat.KORAPXML && annotationWorkerPool == null) {
            LOGGER.fine("Closing output ZIP file in processZipFile (no annotation worker pool)")
            morphoZipOutputStream!!.close()
        } else if (outputFormat == OutputFormat.KORAPXML) {
            LOGGER.fine("NOT closing ZIP in processZipFile - will close after worker pool finishes")
        }
        logZipProgress(zipFilePath)
    }

    private fun processZipFileSequentially(zipFilePath: String, foundry: String = "base") {
        val ord = zipOrdinals[zipFilePath] ?: 0
        val size = zipSizes[zipFilePath] ?: 0L
        LOGGER.info("Processing zip ${if (ord>0) ord else "?"}/$totalZips: ${zipFilePath} (${humanBytes(size)}) in thread ${Thread.currentThread().threadId()}")
        if (zipFilePath.hasCorrespondingBaseZip()) {
            // Process the two related zips strictly sequentially to limit memory growth
            val zips = arrayOf(zipFilePath, zipFilePath.correspondingBaseZip()!!)
            zips.forEach { zip ->
                ZipFile(zip).use { zipFile ->
                    // Iterate entries in a deterministic order to keep related files close together
                    zipFile.stream()
                        .filter { extractMetadataRegex.isNotEmpty() || !it.name.contains("header.xml") }
                        .sorted(Comparator.comparing<ZipEntry, String> { it.name })
                        .forEachOrdered { zipEntry ->
                            processZipEntry(zipFile, foundry, zipEntry, true)
                        }
                }
            }
        } else {
            ZipFile(zipFilePath).use { zipFile ->
                zipFile.stream()
                    .filter { extractMetadataRegex.isNotEmpty() || !it.name.contains("header.xml") }
                    .sorted(Comparator.comparing<ZipEntry, String> { it.name })
                    .forEachOrdered { zipEntry ->
                        processZipEntry(zipFile, foundry, zipEntry, false)
                    }
            }
        }
        logZipProgress(zipFilePath)
    }

    private fun logZipProgress(zipFilePath: String) {
        try {
            val size = zipSizes[zipFilePath] ?: 0L
            val done = processedZipBytes.addAndGet(size)
            val total = if (totalZipBytes > 0) totalZipBytes else 1L
            val elapsedMs = (System.currentTimeMillis() - startTimeMillis).coerceAtLeast(1)
            val speedBytesPerSec = (done * 1000.0) / elapsedMs
            val remaining = (total - done).coerceAtLeast(0)
            val etaSeconds = if (speedBytesPerSec > 0.0) (remaining / speedBytesPerSec).toLong() else -1L
            val ord = zipOrdinals[zipFilePath] ?: 0
            val pct = (done * 100.0 / total).coerceIn(0.0, 100.0)
            val humanSpeed = String.format(Locale.ROOT, "%.2f MB/s", speedBytesPerSec / (1024.0 * 1024.0))
            val etaStr = if (etaSeconds >= 0) formatDuration(etaSeconds) else "unknown"
            LOGGER.info(
                "Finished zip ${if (ord>0) ord else "?"}/$totalZips: ${zipFilePath} " +
                        "(${humanBytes(size)}). Progress: ${String.format(Locale.ROOT, "%.1f", pct)}%%, " +
                        "ETA ${etaStr} at ${humanSpeed}"
            )
        } catch (e: Exception) {
            LOGGER.fine("Failed to log zip progress for $zipFilePath: ${e.message}")
        }
    }

    private fun humanBytes(bytes: Long): String {
        if (bytes < 1024) return "$bytes B"
        val kb = bytes / 1024.0
        if (kb < 1024) return String.format(Locale.ROOT, "%.1f KB", kb)
        val mb = kb / 1024.0
        if (mb < 1024) return String.format(Locale.ROOT, "%.1f MB", mb)
        val gb = mb / 1024.0
        return String.format(Locale.ROOT, "%.1f GB", gb)
    }

    private fun formatDuration(seconds: Long): String {
        var s = seconds
        val h = s / 3600; s %= 3600
        val m = s / 60; val sec = s % 60
        return String.format(Locale.ROOT, "%02d:%02d:%02d", h, m, sec)
    }

    private fun processZipEntriesWithPool(zipFile: ZipFile, foundry: String, waitForMorpho: Boolean) {
        // Collect entries first to avoid lazy evaluation surprises, filter header.xml unless metadata extraction is requested
        val entries: MutableList<ZipEntry> = ArrayList()
        var documentCount = 0
        val enumEntries = zipFile.entries()
        while (enumEntries.hasMoreElements()) {
            val e = enumEntries.nextElement()
            if (extractMetadataRegex.isEmpty() && e.name.contains("header.xml")) continue
            entries.add(e)
            // Count data.xml files as documents for progress tracking
            if (e.name.contains("data.xml")) {
                documentCount++
            }
        }
        if (entries.isEmpty()) return

        // Update total document count and start timer if this is the first ZIP with external annotation
        if (annotationWorkerPool != null && documentCount > 0) {
            val newTotal = totalDocsInInput.addAndGet(documentCount)
            if (annotationStartTime.get() == 0L) {
                annotationStartTime.set(System.currentTimeMillis())
                LOGGER.info("Starting annotation of $newTotal document(s)")

                // Initialize progress bar for external annotation with ZIP output
                progressBar = ProgressBarBuilder()
                    .setTaskName("Annotating")
                    .setInitialMax(newTotal.toLong())
                    .setStyle(ProgressBarStyle.ASCII)
                    .setUpdateIntervalMillis(500) // Update every 500ms
                    .showSpeed()
                    .build()
            }
        }

        // If only one thread requested, do sequential to avoid pool overhead
        if (maxThreads <= 1) {
            entries.forEach { entry -> processZipEntry(zipFile, foundry, entry, waitForMorpho) }
            return
        }

        // Submit all entry tasks to the shared executor and await completion before closing the zip
        val latch = java.util.concurrent.CountDownLatch(entries.size)
        entries.forEach { entry ->
            entryExecutor?.execute {
                try {
                    processZipEntry(zipFile, foundry, entry, waitForMorpho)
                } catch (t: Throwable) {
                    LOGGER.warning("Failed to process entry ${entry.name}: ${t.message}")
                } finally {
                    latch.countDown()
                }
            }
        }
        try {
            latch.await()
        } catch (ie: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    fun processZipEntry(zipFile: ZipFile, _foundry: String, zipEntry: ZipEntry, passedWaitForMorpho: Boolean) {
        var foundry = _foundry
        var waitForMorpho = passedWaitForMorpho
        LOGGER.finer("Processing ${zipEntry.name} in thread ${Thread.currentThread().threadId()}")
        if (taggerName != null && !taggerToolBridges.containsKey(Thread.currentThread().threadId())) {
            val tagger = AnnotationToolBridgeFactory.getAnnotationToolBridge(taggerName!!, taggerModel!!, LOGGER) as TaggerToolBridge?
            taggerToolBridges[Thread.currentThread().threadId()] = tagger
            if (tagger != null) {
                foundry = tagger.foundry
            }

        }
        if (parserName != null && !parserToolBridges.containsKey(Thread.currentThread().threadId())) {
            val parser = AnnotationToolBridgeFactory.getAnnotationToolBridge(parserName!!, parserModel!!, LOGGER) as ParserToolBridge?
            parserToolBridges[Thread.currentThread().threadId()] = parser
            if (parser != null) {
                foundry = "$foundry dependency:${parser.foundry}"
                LOGGER.fine("Initialized parser ${parserName} with foundry $foundry in thread ${Thread.currentThread().threadId()}")
            }
        }

        try {
            if (zipEntry.name.matches(Regex(".*(data|tokens|structure|morpho)\\.xml$"))) {
                // Ensure the entry stream and reader are closed to avoid native memory buildup
                val dbFactory: DocumentBuilderFactory = DocumentBuilderFactory.newInstance()
                val dBuilder: DocumentBuilder = dbFactory.newDocumentBuilder()
                // In lemma-only mode, skip parsing data.xml entirely to reduce memory pressure
                if (lemmaOnly && zipEntry.name.endsWith("data.xml")) {
                    return
                }
                val doc: Document = try {
                    zipFile.getInputStream(zipEntry).use { inputStream ->
                        XMLCommentFilterReader(inputStream, "UTF-8").use { reader ->
                            dBuilder.parse(InputSource(reader))
                        }
                    }
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
                        if (!lemmaOnly) {
                            val textsList: NodeList = doc.getElementsByTagName("text")
                            if (textsList.length > 0) {
                                texts[docId] = NonBmpString(textsList.item(0).textContent)
                            }
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

                val morphoRequired = waitForMorpho || useLemma || taggerName != null || parserName != null || (outputFormat == OutputFormat.KORAPXML && annotationWorkerPool == null)
                // For lemma-only/lemma-based word2vec/now, we can proceed without full text
                val textRequired = when (outputFormat) {
                    OutputFormat.WORD2VEC, OutputFormat.NOW -> !(useLemma || lemmaOnly)
                    else -> true
                }

                LOGGER.fine("Checking if ready to process $docId: texts=${texts[docId] != null}, sentences=${sentences[docId] != null}, tokens=${tokens[docId] != null}, morpho=${morpho[docId] != null}, morphoRequired=$morphoRequired, textRequired=$textRequired, annotationWorkerPool=${annotationWorkerPool != null}")

                if ((texts[docId] != null || !textRequired) && sentences[docId] != null && tokens[docId] != null
                    && (!morphoRequired || morpho[docId] != null)
                    && (extractMetadataRegex.isEmpty() || metadata[docId] != null)
                ) {
                    LOGGER.fine("All data ready for $docId, calling processText")
                    processText(docId, foundry)
                } else {
                    LOGGER.fine("NOT ready to process $docId yet: textOK=${texts[docId] != null || !textRequired}, sentencesOK=${sentences[docId] != null}, tokensOK=${tokens[docId] != null}, morphoOK=${!morphoRequired || morpho[docId] != null}")
                }
            } else if (extractMetadataRegex.isNotEmpty() && zipEntry.name.matches(Regex(".*/header\\.xml$"))) {
                //LOGGER.info("Processing header file: " + zipEntry.name)
                val text = zipFile.getInputStream(zipEntry).bufferedReader().use { it.readText() }
                val docId =
                    Regex("<textSigle>([^<]+)</textSigle>").find(text)?.destructured?.component1()
                        ?.replace(Regex("/"), "_")
                LOGGER.fine("Processing header file: " + zipEntry.name + " docId: " + docId)
                val meta = ArrayList<String>()
                extractMetadataRegex.forEach { regex ->
                    val match = Regex(regex).find(text)
                    if (match != null) {
                        meta.add(match.destructured.component1())
                    }
                }
                if (meta.isNotEmpty() && docId != null) {
                    metadata[docId] = meta.toTypedArray()
                    val morphoRequired = waitForMorpho || useLemma || taggerName != null || parserName != null || (outputFormat == OutputFormat.KORAPXML && annotationWorkerPool == null)
                    val textRequired = when (outputFormat) {
                        OutputFormat.WORD2VEC, OutputFormat.NOW -> !(useLemma || lemmaOnly)
                        else -> true
                    }
                    if ((texts[docId] != null || !textRequired) && sentences[docId] != null && tokens[docId] != null
                         && (!morphoRequired || morpho[docId] != null)
                     ) {
                        // Be quiet on INFO; per-text logs only on FINE and below
                        LOGGER.info("Processing text (meta-ready): $docId in thread ${Thread.currentThread().threadId()}")
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
        LOGGER.fine("Processing text: $docId in thread ${Thread.currentThread().threadId()}")
        var morphoFoundry = getMorphoFoundry()
        val output =
        if (outputFormat == OutputFormat.WORD2VEC) {
            lmTrainingOutput(docId)
        } else if (outputFormat == OutputFormat.NOW) {
            nowOutput(docId)
        } else {
            if (taggerToolBridges[Thread.currentThread().threadId()] != null) {
                morpho[docId] = taggerToolBridges[Thread.currentThread().threadId()]!!.tagText(
                    tokens[docId]!!,
                    sentences[docId],
                    texts[docId]!!
                )

            }
            if (parserToolBridges[Thread.currentThread().threadId()] != null) {
                if (morpho[docId] == null) {
                    LOGGER.severe("No morpho data for $docId")
                    //exitProcess(1)
                }
                LOGGER.finer("Parsing text: $docId in thread ${Thread.currentThread().threadId()}")
                morpho[docId] = parserToolBridges[Thread.currentThread().threadId()]!!.parseText(
                    tokens[docId]!!,
                    morpho[docId],
                    sentences[docId],
                    texts[docId]!!
                )
                LOGGER.finer("Parsed text: $docId in thread ${Thread.currentThread().threadId()}")
            }
            if (outputFormat == OutputFormat.KORAPXML && annotationWorkerPool == null) {
                korapXmlOutput(getMorphoFoundry(), docId)
            } else {
                conlluOutput(foundry, docId)
            }
        }

        if (annotationWorkerPool != null) {
            if (outputFormat == OutputFormat.KORAPXML) {
                // Store metadata in task, send clean CoNLL-U to external process
                val entryPath = if (parserName != null)  docId.replace(Regex("[_.]"), "/").plus("/$parserName/").plus("dependency.xml")
                else
                    docId.replace(Regex("[_.]"), "/").plus("/$morphoFoundry/").plus("morpho.xml")
                LOGGER.fine("Sending document $docId (${output.length} chars) to annotation worker pool for ZIP output")
                // Pass metadata via AnnotationTask, NOT in the text itself
                annotationWorkerPool?.pushToQueue(output.toString(), docId, entryPath + "|" + foundry)
                docsSentToAnnotation.incrementAndGet()
            } else {
                LOGGER.fine("Sending document $docId (${output.length} chars) to annotation worker pool")
                annotationWorkerPool?.pushToQueue(output.toString())
                docsSentToAnnotation.incrementAndGet()
            }
            // Release internal char[] early
            output.setLength(0)
        } else if (outputFormat != OutputFormat.KORAPXML) {
            synchronized(System.out) {
                println(output.toString())
            }
            // Release internal char[] early
            output.setLength(0)
        } else {
            // Direct ZIP output without external annotation
            val entryPath = if (parserName != null)  docId.replace(Regex("[_.]"), "/").plus("/$parserName/").plus("dependency.xml")
            else
                docId.replace(Regex("[_.]"), "/").plus("/$morphoFoundry/").plus("morpho.xml")
            val zipEntry = ZipArchiveEntry(entryPath)
            zipEntry.unixMode = ZIP_ENTRY_UNIX_MODE
            synchronized(morphoZipOutputStream!!) {
                morphoZipOutputStream!!.putArchiveEntry(zipEntry)
                morphoZipOutputStream!!.write(output.toString().toByteArray())
                morphoZipOutputStream!!.closeArchiveEntry()
            }
            output.clear()
        }


        arrayOf(tokens, texts, sentences, morpho, fnames, metadata, extraFeatures).forEach { map ->
            if (map === morpho) {
                // Clear inner map to release references early
                morpho[docId]?.clear()
            }
            map.remove(docId)
        }

        // Periodic GC hint after processing many docs (lightweight safeguard)
        if ((processedDocs.incrementAndGet() % 2000) == 0) {
            LOGGER.fine("Processed ${processedDocs.get()} docs – requesting GC hint")
            System.gc()
        }
        // Memory / cache statistics logging
        if (memStatsInterval > 0) {
            val count = processedDocs.get()
            if (count % memStatsInterval == 0) {
                logMemoryStats(count)
            }
        }
    }

    private fun getMorphoFoundry() = taggerToolBridges[Thread.currentThread().threadId()]?.foundry ?: "base"

    private fun logMemoryStats(count: Int) {
        try {
            val rt = Runtime.getRuntime()
            val used = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024)
            val total = rt.totalMemory() / (1024 * 1024)
            val max = rt.maxMemory() / (1024 * 1024)
            LOGGER.info(
                "MEM-STATS docs=${count} usedMB=${used} totalMB=${total} maxMB=${max} " +
                        "maps{texts=${texts.size},tokens=${tokens.size},sentences=${sentences.size},morpho=${morpho.size}}"
            )
        } catch (e: Exception) {
            LOGGER.warning("Failed to log memory stats: ${e.message}")
        }
    }

    private fun korapXmlDependencyOutput(foundry: String, docId: String): StringBuilder {
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
        var s = 0
        var n = 0
        val sortedKeys = morpho[docId]?.keys?.sortedBy { it.split("-")[0].toInt() }

        sortedKeys?.forEach { spanString ->
            val mfs = morpho[docId]?.get(spanString)
            val offsets = spanString.split("-")
            if(offsets.size != 2) {
                LOGGER.warning("Invalid span: $spanString in $docId")
                return@forEach
            }
            if (offsets[0].toInt() > sentences[docId]!!.elementAt(s).to) {
                s++
                n = i
            }
            i++
            if (mfs!!.deprel == "_") {
                return@forEach
            }

            val spanNode = doc.createElement("span")
            spanNode.setAttribute("id", "s${s + 1}_n${i - n}")
            spanNode.setAttribute("from", offsets[0])
            spanNode.setAttribute("to", offsets[1])

            // rel element
            val rel = doc.createElement("rel")
            rel.setAttribute("label", mfs.deprel)

            // inner span element
            val innerSpan = doc.createElement("span")
            val headInt = if(mfs.head == "_") 0 else parseInt(mfs.head) - 1
            if (headInt < 0) {
                innerSpan.setAttribute("from", sentences[docId]!!.elementAt(s).from.toString())
                innerSpan.setAttribute("to",  sentences[docId]!!.elementAt(s).to.toString())
            } else {
                if (headInt + n >= morpho[docId]!!.size) {
                    LOGGER.warning("Head index out of bounds: ${headInt+n} >= ${morpho[docId]!!.size} in $docId")
                    return@forEach
                } else {
                    val destSpanString = sortedKeys.elementAt(headInt + n)
                    val destOffsets = destSpanString.split("-")
                    innerSpan.setAttribute("from", destOffsets[0])
                    innerSpan.setAttribute("to", destOffsets[1])
                }
            }
            rel.appendChild(innerSpan)
            spanNode.appendChild(rel)
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

    private fun korapXmlOutput(foundry: String, docId: String): StringBuilder {
        return if (parserName != null) {
            korapXmlDependencyOutput(foundry, docId)
        } else {
            korapXmlMorphoOutput(foundry, docId)
        }
    }

    private fun korapXmlMorphoOutput(foundry: String, docId: String): StringBuilder {
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
        val sentencesArr = sentences[docId]
        val tokensArr = tokens[docId]
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
        if (tokensArr == null || tokensArr.isEmpty()) {
            return output
        }
        val textVal = texts[docId]
        tokensArr.forEach { span ->
            token_index++
            if (sentencesArr != null && (sentence_index >= sentencesArr.size || span.from >= sentencesArr[sentence_index].to)) {
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
            // Bestimme den Token-Text sicher
            var tokenText: String = if (textVal != null) {
                val safeFrom = span.from.coerceIn(0, textVal.length)
                val safeTo = span.to.coerceIn(safeFrom, textVal.length)
                textVal.substring(safeFrom, safeTo)
            } else "_"

            // Validate and fix empty/whitespace-only tokens that cause SpaCy to crash
            if (tokenText.isBlank()) {
                LOGGER.fine("Replacing empty/blank token at offset ${span.from}-${span.to} in document $docId with underscore")
                tokenText = "_"  // Replace with underscore instead of skipping
            }

            if (morpho[docId]?.containsKey("${span.from}-${span.to}") == true) {
                val mfs = morpho[docId]?.get("${span.from}-${span.to}")
                if (mfs != null) {
                    // Add offset info to MISC field for external annotation with ZIP output
                    val miscWithOffset = if (annotationWorkerPool != null && outputFormat == OutputFormat.KORAPXML) {
                        val existing = mfs.misc ?: "_"
                        if (existing == "_") "Offset=${span.from}-${span.to}"
                        else "${existing}|Offset=${span.from}-${span.to}"
                    } else mfs.misc ?: "_"

                    try {
                        output.append(
                            printConlluToken(
                                token_index,
                                tokenText,
                                mfs.lemma ?: "_",
                                mfs.upos ?: "_",
                                mfs.xpos ?: "_",
                                mfs.feats ?: "_",
                                mfs.head ?: "_",
                                mfs.deprel ?: "_",
                                mfs.deps ?: "_",
                                miscWithOffset,
                                columns
                            )
                        )
                    } catch (e: NullPointerException) {
                        LOGGER.warning("NPE processing morpho for $docId at ${span.from}-${span.to}: ${e.message}")
                        // Fallback to token without morpho
                        val miscWithOffset = if (annotationWorkerPool != null && outputFormat == OutputFormat.KORAPXML) {
                            "Offset=${span.from}-${span.to}"
                        } else "_"
                        output.append(
                            printConlluToken(
                                token_index, tokenText, misc = miscWithOffset, columns = columns
                            )
                        )
                    }
                } else {
                    // Fallback if mfs is null
                    val miscWithOffset = if (annotationWorkerPool != null && outputFormat == OutputFormat.KORAPXML) {
                        "Offset=${span.from}-${span.to}"
                    } else "_"

                    output.append(
                        printConlluToken(
                            token_index, tokenText, misc = miscWithOffset, columns = columns
                        )
                    )
                }
            } else {
                // Add offset info for tokens without morpho data when needed
                val miscWithOffset = if (annotationWorkerPool != null && outputFormat == OutputFormat.KORAPXML) {
                    "Offset=${span.from}-${span.to}"
                } else "_"

                output.append(
                    printConlluToken(
                        token_index, tokenText, misc = miscWithOffset, columns = columns
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
        // If no text is available (e.g., lemma-only mode), emit lemmas
        if (texts[docId] == null) {
            tokens[docId]?.forEach { span ->
                val key = "${span.from}-${span.to}"
                val lemmaVal = morpho[docId]?.get(key)?.lemma
                output.append((lemmaVal?.takeIf { it != "_" } ?: "_"), " ")
            }
            if (output.isNotEmpty()) output.deleteCharAt(output.length - 1)
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
            // Bounds safety
            val safeFrom = span.from.coerceIn(0, texts[docId]!!.length)
            val safeTo = span.to.coerceIn(safeFrom, texts[docId]!!.length)
            if (useLemma && morpho[docId] != null) {
                val key = "${span.from}-${span.to}"
                val lemmaVal = morpho[docId]!![key]?.lemma
                if (lemmaVal != null && lemmaVal != "_") {
                    output.append(lemmaVal)
                    output.append(' ')
                } else {
                    texts[docId]!!.appendRangeTo(output, safeFrom, safeTo)
                    output.append(' ')
                }
            } else {
                texts[docId]!!.appendRangeTo(output, safeFrom, safeTo)
                output.append(' ')
            }
            real_token_index++
        }
        if (output.isNotEmpty()) {
            output.deleteCharAt(output.length - 1)
        }
        return output
    }

    private fun nowOutput(docId: String): StringBuilder {
        var token_index = 0
        var real_token_index = 0
        var sentence_index = 0
        val output: StringBuilder = StringBuilder()

        // Add the text sigle prefix
        output.append("@@$docId ")

        if (texts[docId] == null) {
            // Lemma-only fallback when original text is not loaded
            tokens[docId]?.forEach { span ->
                if (sentences[docId] != null && (sentence_index >= sentences[docId]!!.size || span.from >= sentences[docId]!![sentence_index].to)) {
                    if (output.isNotEmpty() && !output.endsWith("@@$docId ")) {
                        output.append(" <p> ")
                    }
                    sentence_index++
                }
                val key = "${span.from}-${span.to}"
                val lemmaVal = morpho[docId]?.get(key)?.lemma
                output.append((lemmaVal?.takeIf { it != "_" } ?: "_"), " ")
            }
            if (output.isNotEmpty() && output.endsWith(" ")) {
                output.deleteCharAt(output.length - 1)
            }
            return output
        }
        
        tokens[docId]?.forEach { span ->
            token_index++
            if (sentences[docId] != null && (sentence_index >= sentences[docId]!!.size || span.from >= sentences[docId]!![sentence_index].to)) {
                // Replace sentence end with <p> tag instead of newline
                if (output.isNotEmpty() && !output.endsWith("@@$docId ")) {
                    output.append(" <p> ")
                }
                sentence_index++
            }
            // Bounds safety
            val safeFrom = span.from.coerceIn(0, texts[docId]!!.length)
            val safeTo = span.to.coerceIn(safeFrom, texts[docId]!!.length)
            if (useLemma && morpho[docId] != null) {
                val key = "${span.from}-${span.to}"
                val lemmaVal = morpho[docId]!![key]?.lemma
                if (lemmaVal != null && lemmaVal != "_") {
                    output.append(lemmaVal)
                    output.append(' ')
                } else {
                    texts[docId]!!.appendRangeTo(output, safeFrom, safeTo)
                    output.append(' ')
                }
            } else {
                texts[docId]!!.appendRangeTo(output, safeFrom, safeTo)
                output.append(' ')
            }
            real_token_index++
        }
        
        // Remove trailing space and add final newline
        if (output.isNotEmpty() && output.endsWith(" ")) {
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
            else -> {
                val fields = listOf(
                    token_index.toString(), token, lemma, myUpos, xpos, feats, head, deprel, deps, misc
                )
                fields.subList(0, min(columns, 10)).joinToString("\t", postfix = tokenSeparator)
            }
        }
    }

    private fun tokenOffsetsInSentence(
        sentences: ConcurrentHashMap<String, Array<Span>>,
        docId: String,
        sentence_index: Int,
        token_index: Int,
        tokens: ConcurrentHashMap<String, Array<Span>>
    ): String {
        val sentArr = sentences[docId] ?: return ""
        if (sentence_index !in sentArr.indices) return ""
        val toks = tokens[docId] ?: return ""
        if (toks.isEmpty() || token_index !in toks.indices) return ""
        val sentenceEndOffset = sentArr[sentence_index].to
        var i = token_index
        val start_offsets_string = StringBuilder()
        val end_offsets_string = StringBuilder()
        while (i < toks.size && toks[i].to <= sentenceEndOffset) {
            start_offsets_string.append(" ").append(toks[i].from)
            end_offsets_string.append(" ").append(toks[i].to)
            i++
        }
        return StringBuilder()
            .append("# start_offsets = ").append(toks[token_index].from).append(start_offsets_string).append("\n")
            .append("# end_offsets = ").append(sentenceEndOffset).append(end_offsets_string).append("\n")
            .toString()
    }

    private fun extractSpans(spans: NodeList): Array<Span> {
        val list = ArrayList<Span>()
        IntStream.range(0, spans.length).forEach { idx ->
            val node = spans.item(idx)
            if (node is Element) {
                val fromAttr = node.getAttribute("from")
                val toAttr = node.getAttribute("to")
                if (fromAttr.isNullOrEmpty() || toAttr.isNullOrEmpty()) {
                    LOGGER.warning("Skipping span with empty from/to attribute: from='$fromAttr' to='$toAttr'")
                } else {
                    try {
                        val from = Integer.parseInt(fromAttr)
                        val to = Integer.parseInt(toAttr)
                        list.add(Span(from, to))
                    } catch (e: NumberFormatException) {
                        LOGGER.warning("Skipping span with invalid numeric offsets: from='$fromAttr' to='$toAttr' : ${e.message}")
                    }
                }
            }
        }
        return list.toTypedArray()
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

    private fun parseAndWriteAnnotatedConllu(annotatedConllu: String, task: AnnotationWorkerPool.AnnotationTask?) {
        LOGGER.fine("parseAndWriteAnnotatedConllu called with ${annotatedConllu.length} chars, task=$task")

        // Extract metadata from task
        val docId = task?.docId
        val entryPathAndFoundry = task?.entryPath?.split("|") ?: listOf(null, null)
        val entryPath = entryPathAndFoundry.getOrNull(0)
        val foundry = entryPathAndFoundry.getOrNull(1) ?: "base"

        if (docId == null || entryPath == null) {
            LOGGER.fine("Missing metadata from task! docId=$docId, entryPath=$entryPath, task=$task")
            return
        }

        LOGGER.fine("Processing annotated document: docId=$docId, entryPath=$entryPath, foundry=$foundry")

        val morphoSpans = mutableMapOf<String, MorphoSpan>()

        // Parse the annotated CoNLL-U to extract morpho data
        val lines = annotatedConllu.lines()
        var currentStartOffsets: List<Int>? = null
        var currentEndOffsets: List<Int>? = null
        var tokenIndexInSentence = 0

        for (line in lines) {
            when {
                line.startsWith("# start_offsets =") -> {
                    // Parse start offsets from comment
                    // Format: # start_offsets = <first_token_from> <token1_from> <token2_from> ...
                    // The first value is duplicated, so skip it and use values from index 1 onwards
                    val offsetsStr = line.substring("# start_offsets =".length).trim()
                    val allOffsets = offsetsStr.split(Regex("\\s+")).mapNotNull { it.toIntOrNull() }
                    currentStartOffsets = if (allOffsets.size > 1) allOffsets.drop(1) else allOffsets
                    tokenIndexInSentence = 0
                    LOGGER.fine("Found start offsets: $currentStartOffsets")
                }
                line.startsWith("# end_offsets =") -> {
                    // Parse end offsets from comment
                    // Format: # end_offsets = <sentence_end> <token1_to> <token2_to> ...
                    // First value is sentence end, actual token ends start from index 1
                    val offsetsStr = line.substring("# end_offsets =".length).trim()
                    val allOffsets = offsetsStr.split(Regex("\\s+")).mapNotNull { it.toIntOrNull() }
                    currentEndOffsets = if (allOffsets.size > 1) allOffsets.drop(1) else emptyList()
                    LOGGER.fine("Found end offsets: $currentEndOffsets")
                }
                line.isEmpty() -> {
                    // Reset for next sentence
                    currentStartOffsets = null
                    currentEndOffsets = null
                    tokenIndexInSentence = 0
                }
                !line.startsWith("#") -> {
                    // This is a token line
                    val fields = line.split("\t")
                    if (fields.size < 10) continue

                    val lemma = if (fields.size > 2) fields[2] else "_"
                    val upos = if (fields.size > 3) fields[3] else "_"
                    val xpos = if (fields.size > 4) fields[4] else "_"
                    val feats = if (fields.size > 5) fields[5] else "_"
                    val head = if (fields.size > 6) fields[6] else "_"
                    val deprel = if (fields.size > 7) fields[7] else "_"
                    val deps = if (fields.size > 8) fields[8] else "_"
                    val misc = if (fields.size > 9) fields[9] else "_"

                    // Get offset from the comment-based offset arrays
                    if (currentStartOffsets != null && currentEndOffsets != null &&
                        tokenIndexInSentence < currentStartOffsets.size &&
                        tokenIndexInSentence < currentEndOffsets.size) {

                        val spanFrom = currentStartOffsets[tokenIndexInSentence]
                        val spanTo = currentEndOffsets[tokenIndexInSentence]
                        val spanKey = "$spanFrom-$spanTo"

                        morphoSpans[spanKey] = MorphoSpan(lemma, upos, xpos, feats, head, deprel, deps, misc)
                        LOGGER.fine("Added morpho span: $spanKey -> $lemma/$upos")
                        tokenIndexInSentence++
                    } else {
                        LOGGER.fine("No offset information for token at index $tokenIndexInSentence in sentence (starts=${currentStartOffsets?.size}, ends=${currentEndOffsets?.size})")
                    }
                }
            }
        }

        LOGGER.fine("Extracted ${morphoSpans.size} morpho spans for $docId")

        if (morphoSpans.isEmpty()) {
            LOGGER.warning("No morpho spans found in annotated output for $docId, skipping")
            LOGGER.warning("Sample lines: ${lines.take(10).joinToString("\\n")}")
            return
        }

        // Check if the data contains dependency information (non-empty head/deprel fields)
        val hasDependencies = morphoSpans.values.any { span ->
            span.head != null && span.head != "_" && span.deprel != null && span.deprel != "_"
        }
        LOGGER.fine("Document has dependencies: $hasDependencies")

        if (morphoZipOutputStream == null) {
            LOGGER.severe("morphoZipOutputStream is null! Cannot write to ZIP. This should have been initialized in processZipFile.")
            return
        }

        if (dBuilder == null) {
            LOGGER.warning("dBuilder is null, initializing now...")
            dbFactory = DocumentBuilderFactory.newInstance()
            dBuilder = dbFactory!!.newDocumentBuilder()
        }

        // Create a temporary document context for korapXmlOutput
        // Store the morpho data temporarily and copy necessary supporting data
        val tempDocId = "_temp_annotated_$docId"
        morpho[tempDocId] = morphoSpans

        // For dependency output, we need sentences data
        // Create dummy sentence spans covering the entire document based on tokens
        if (hasDependencies && morphoSpans.isNotEmpty()) {
            // Get min and max offsets from all tokens
            val allOffsets = morphoSpans.keys.map { key ->
                val parts = key.split("-")
                Pair(parts[0].toInt(), parts[1].toInt())
            }
            val minOffset = allOffsets.minOfOrNull { it.first } ?: 0
            val maxOffset = allOffsets.maxOfOrNull { it.second } ?: 0

            // Create a single sentence span covering all tokens
            // This is a simplification - ideally we'd track sentence boundaries from CoNLL-U
            sentences[tempDocId] = arrayOf(Span(minOffset, maxOffset))
        }

        LOGGER.fine("Generating KorapXML for $docId with ${morphoSpans.size} spans")

        // Generate morpho.xml (always)
        try {
            val morphoXmlOutput = korapXmlMorphoOutput(foundry, tempDocId)

            // Fix the docid attribute - replace temp prefix with actual docId
            val fixedMorphoXml = morphoXmlOutput.toString().replace(
                "docid=\"$tempDocId\"",
                "docid=\"$docId\""
            )

            val morphoEntryPath = docId.replace(Regex("[_.]"), "/") + "/$foundry/morpho.xml"

            val morphoZipEntry = ZipArchiveEntry(morphoEntryPath)
            morphoZipEntry.unixMode = ZIP_ENTRY_UNIX_MODE
            LOGGER.fine("Writing ${fixedMorphoXml.length} bytes to ZIP entry: $morphoEntryPath")
            synchronized(morphoZipOutputStream!!) {
                morphoZipOutputStream!!.putArchiveEntry(morphoZipEntry)
                morphoZipOutputStream!!.write(fixedMorphoXml.toByteArray())
                morphoZipOutputStream!!.closeArchiveEntry()
            }
            LOGGER.fine("Successfully wrote morpho.xml for $docId")
            val written = docsWrittenToZip.incrementAndGet()

            // Update progress bar
            progressBar?.step()

            // Show progress with ETA at INFO level
            if (annotationWorkerPool != null && totalDocsInInput.get() > 0) {
                val total = totalDocsInInput.get()
                val percent = (written * 100.0) / total
                val elapsed = (System.currentTimeMillis() - annotationStartTime.get()) / 1000.0
                val docsPerSec = if (elapsed > 0) written / elapsed else 0.0
                val remaining = total - written
                val etaSec = if (docsPerSec > 0) remaining / docsPerSec else 0.0

                // Calculate estimated finish time
                val finishTime = LocalDateTime.now().plusSeconds(etaSec.toLong())
                val timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")

                if (written % 10 == 0 || written == total) {
                    val etaMin = (etaSec / 60).toInt()
                    val etaSec2 = (etaSec % 60).toInt()
                    LOGGER.info(String.format(Locale.ROOT,
                        "Progress: %d/%d (%.1f%%), %.1f docs/s, ETA %02d:%02d, finish ~%s",
                        written, total, percent, docsPerSec, etaMin, etaSec2, finishTime.format(timeFormatter)))
                }
            }
        } catch (e: Exception) {
            LOGGER.severe("ERROR generating/writing morpho.xml: ${e.message}")
            e.printStackTrace()
        }

        // Generate dependency.xml if dependencies are present
        if (hasDependencies) {
            try {
                val dependencyXmlOutput = korapXmlDependencyOutput(foundry, tempDocId)

                // Fix the docid attribute - replace temp prefix with actual docId
                val fixedDependencyXml = dependencyXmlOutput.toString().replace(
                    "docid=\"$tempDocId\"",
                    "docid=\"$docId\""
                )

                val dependencyEntryPath = docId.replace(Regex("[_.]"), "/") + "/$foundry/dependency.xml"

                val dependencyZipEntry = ZipArchiveEntry(dependencyEntryPath)
                dependencyZipEntry.unixMode = ZIP_ENTRY_UNIX_MODE
                LOGGER.fine("Writing ${fixedDependencyXml.length} bytes to ZIP entry: $dependencyEntryPath")
                synchronized(morphoZipOutputStream!!) {
                    morphoZipOutputStream!!.putArchiveEntry(dependencyZipEntry)
                    morphoZipOutputStream!!.write(fixedDependencyXml.toByteArray())
                    morphoZipOutputStream!!.closeArchiveEntry()
                }
                LOGGER.fine("Successfully wrote dependency.xml for $docId")
            } catch (e: Exception) {
                LOGGER.severe("ERROR generating/writing dependency.xml: ${e.message}")
                e.printStackTrace()
            }
        }

        // Clean up temporary data
        morpho.remove(tempDocId)
        sentences.remove(tempDocId)
    }

}

fun main(args: Array<String>): Unit = exitProcess(CommandLine(KorapXmlTool()).execute(*args))

fun debug(args: Array<String>): Int {
    return (CommandLine(KorapXmlTool()).execute(*args))
}

enum class OutputFormat {
    CONLLU, WORD2VEC, KORAPXML, NOW
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

object NowOutputFormat {
    const val NAME = "now"
}

