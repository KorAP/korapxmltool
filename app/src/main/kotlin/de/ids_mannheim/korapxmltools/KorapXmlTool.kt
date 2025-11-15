package de.ids_mannheim.korapxmltools

import de.ids_mannheim.korapxmltools.AnnotationToolBridgeFactory.Companion.parserFoundries
import de.ids_mannheim.korapxmltools.AnnotationToolBridgeFactory.Companion.taggerFoundries
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.archivers.zip.Zip64Mode
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
import org.apache.commons.compress.archivers.zip.ZipFile as ApacheZipFile
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import org.xml.sax.SAXParseException
import picocli.CommandLine
import picocli.CommandLine.*
import java.io.*
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
import java.util.zip.GZIPOutputStream
import kotlin.text.Charsets
import me.tongfei.progressbar.ProgressBar
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import javax.xml.XMLConstants
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
    version = ["KorapXmlTool 2.20"],
    description = ["Converts KorAP-XML <https://github.com/KorAP/KorAP-XML-Krill#about-korap-xml> base or " +
            "morpho zips to (annotated) CoNLL(-U) format with all information necessary for " +
            "reconstruction in comment lines."]
)

class KorapXmlTool : Callable<Int> {
    val COMPATIBILITY_MODE = System.getenv("COMPATIBILITY_MODE") != null

    @Spec lateinit var spec : Model.CommandSpec

    // When using --annotate-with, hold the external tool's foundry label (e.g., spacy, stanza)
    private var externalFoundry: String? = null
    // Target ZIP filename (when writing ZIP output); used to label the progress bar
    private var targetZipFileName: String? = null
    // Locale is now globally forced to ROOT at startup (see main())

    @Parameters(arity = "1..*", description = ["At least one zip file name"])
    var zipFileNames: Array<String>? = null

    @Option(
        names = ["-f", "--output-format"],
        description = ["Output format: ${ConlluOutputFormat.NAME}, ${Word2VecOutputFormat.NAME}, ${KorapXmlOutputFormat.NAME}, ${NowOutputFormat.NAME}, ${KrillOutputFormat.NAME}",
            "conllu: CoNLL-U format",
            "korapxml, xml, zip: KorAP-XML format zip",
            "word2vec, w2v: Print text in LM training format: tokens separated by space, sentences separated by newlines",
            "now, NOW: NOW corpus export format: w2v-like format with <p> tags for sentence ends and @@<text-sigle> prefix",
            "krill: Krill JSON format (tar file with gzipped JSON files, one per text)",
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
                "krill" -> OutputFormat.KRILL
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

    @Option(
        names = ["--non-word-tokens", "--nwt", "-nwt"],
        description = ["Include punctuation and other non-word tokens when generating Krill output (matches korapxml2krill --non-word-tokens flag)."]
    )
    var includeNonWordTokens: Boolean = false

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
        names = ["--quiet", "-q"],
        description = ["Suppress the visual progress bar (logging remains unchanged)"]
    )
    var quiet: Boolean = false

    @Option(
        names = ["--threads", "-T"],
        paramLabel = "THREADS",
        description = ["Maximum number of threads to use. Default: ${"$"}{DEFAULT-VALUE}"]
    )
    var maxThreads: Int = Runtime.getRuntime().availableProcessors() / 2
    fun setThreads(threads: Int) {
        if (threads < 1) {
            throw ParameterException(spec.commandLine(), String.format(Locale.ROOT, "Invalid value `%d' for option '--threads': must be at least 1", threads))
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
        names = ["-D", "--output-dir"],
        paramLabel = "DIR",
        description = ["Output directory for generated files (default: current directory)"]
    )
    var outputDir: String = "."

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
                String.format(Locale.ROOT, "Invalid value `%s' for option '--tag-with': "+
                    "value does not match the expected pattern ${taggerFoundries}:<path/to/model>", tagWith))
        } else {
            taggerName = matcher.group(1)
            taggerModel = matcher.group(2)
            if (!File(taggerModel!!).exists()) {
                throw ParameterException(spec.commandLine(),
                    String.format(Locale.ROOT, "Invalid value for option '--tag-with':"+
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
                String.format(Locale.ROOT, "Invalid value `%s' for option '--parse-with': "+
                        "value does not match the expected pattern (${parserFoundries}):<path/to/model>", parseWith))
        } else {
            parserName = matcher.group(1)
            parserModel = matcher.group(2)
            if (!File(parserModel!!).exists()) {
                throw ParameterException(spec.commandLine(),
                    String.format(Locale.ROOT, "Invalid value for option '--parse-with':"+
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

        // For krill format, redirect logging to file before any logging occurs
        if (outputFormat == OutputFormat.KRILL) {
            // Find the base ZIP (one without a foundry suffix)
            val baseZip = zipFileNames!!.firstOrNull { zip ->
                val name = File(zip).name
                name.matches(Regex(".*\\.zip$")) && !name.matches(Regex(".*\\.[^/.]+\\.zip$"))
            } ?: zipFileNames!![0]

            val baseZipName = File(baseZip).name.replace(Regex("\\.zip$"), "")
            val krillOutputPath = File(outputDir, "$baseZipName.krill.tar").absolutePath
            val logFilePath = krillOutputPath.replace(Regex("\\.tar$"), ".log")

            // Set up file handler for logging
            val fileHandler = java.util.logging.FileHandler(logFilePath, true)
            fileHandler.formatter = ColoredFormatter()

            // Remove existing console handlers so logs only go to file
            for (logHandler in LOGGER.handlers.toList()) {
                LOGGER.removeHandler(logHandler)
            }
            LOGGER.addHandler(fileHandler)

            // Mirror System.err to the same log file
            val errPs = java.io.PrintStream(java.io.BufferedOutputStream(java.io.FileOutputStream(logFilePath, true)), true)
            val oldErr = System.err
            System.setErr(errPs)

            // Restore System.err and remove file handler on shutdown
            Runtime.getRuntime().addShutdownHook(Thread {
                try {
                    LOGGER.info("Shutting down; closing krill log handler")
                    LOGGER.removeHandler(fileHandler)
                    fileHandler.close()
                } catch (_: Exception) {}
                try { System.setErr(oldErr) } catch (_: Exception) {}
                try { errPs.close() } catch (_: Exception) {}
            })
        }

        LOGGER.info("Processing zip files: " + zipFileNames!!.joinToString(", "))

        korapxml2conllu(zipFileNames!!)
        return 0
    }

    private val LOGGER: Logger = Logger.getLogger(KorapXmlTool::class.java.name)

    private var annotationWorkerPool : AnnotationWorkerPool? = null

    // Track the next text ID (watermark) each foundry needs to process for priority scheduling
    // The foundry with the lexicographically smallest next text ID gets priority
    private val foundryWatermarks: ConcurrentHashMap<String, String> = ConcurrentHashMap()

    // Priority-based task for foundry-aware scheduling
    private inner class PrioritizedTask(
        val foundry: String,
        val textId: String,  // The text ID this task will process
        val task: Runnable,
        val submissionTime: Long = System.nanoTime()
    ) : Comparable<PrioritizedTask>, Runnable {
        override fun compareTo(other: PrioritizedTask): Int {
            // Priority is based on text ID comparison
            // Tasks with lexicographically smaller text IDs get higher priority
            // This keeps all foundries progressing through the corpus together
            // and handles sparse foundries naturally (they won't block on non-existent texts)

            // First, compare text IDs lexicographically
            val textIdDiff = textId.compareTo(other.textId)
            if (textIdDiff != 0) return textIdDiff

            // If same text ID, prefer base foundry (it should be processed first)
            if (foundry == "base" && other.foundry != "base") return -1
            if (foundry != "base" && other.foundry == "base") return 1

            // If same text ID and both base or both non-base, use foundry name
            val foundryDiff = foundry.compareTo(other.foundry)
            if (foundryDiff != 0) return foundryDiff

            // Final tiebreaker: submission time
            return submissionTime.compareTo(other.submissionTime)
        }

        override fun run() {
            task.run()
        }
    }

    // Single priority-based executor for all entry processing
    private var entryExecutor: java.util.concurrent.ExecutorService? = null

    // Extract text ID from ZIP entry path (e.g., "ZGE24/JAN/00001/base/data.xml" -> "ZGE24_JAN.00001")
    private fun getTextIdFromPath(path: String): String {
        val parts = path.split('/')
        return if (parts.size >= 3) {
            "${parts[0]}_${parts[1]}.${parts[2]}"
        } else {
            parts[0]  // Fallback to first component
        }
    }

    val texts: ConcurrentHashMap<String, NonBmpString> = ConcurrentHashMap()
    val sentences: ConcurrentHashMap<String, Array<Span>> = ConcurrentHashMap()
    val tokens: ConcurrentHashMap<String, Array<Span>> = ConcurrentHashMap()
    val morpho: ConcurrentHashMap<String, MutableMap<String, MorphoSpan>> = ConcurrentHashMap()
    val constituencyTrees: ConcurrentHashMap<String, List<ConstituencyParserBridge.ConstituencyTree>> = ConcurrentHashMap()
    val fnames: ConcurrentHashMap<String, String> = ConcurrentHashMap()
    val metadata: ConcurrentHashMap<String, Array<String>> = ConcurrentHashMap()
    val extraFeatures: ConcurrentHashMap<String, MutableMap<String, String>> = ConcurrentHashMap()
    private val processedDocs = java.util.concurrent.atomic.AtomicInteger(0)
    private val docsSentToAnnotation = java.util.concurrent.atomic.AtomicInteger(0)
    private val docsWrittenToZip = java.util.concurrent.atomic.AtomicInteger(0)
    private val totalDocsInInput = java.util.concurrent.atomic.AtomicInteger(0) // Track total documents for progress
    private val annotationStartTime = java.util.concurrent.atomic.AtomicLong(0) // Track when annotation started
    private var progressBar: ProgressBar? = null
    var taggerToolBridges: ConcurrentHashMap<Long, TaggerToolBridge> = ConcurrentHashMap()
    var parserToolBridges: ConcurrentHashMap<Long, ParserToolBridge> = ConcurrentHashMap()
    var constituencyParserBridges: ConcurrentHashMap<Long, ConstituencyParserBridge> = ConcurrentHashMap()

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
    var krillTarOutputStream: TarArchiveOutputStream? = null
    var krillOutputFileName: String? = null

    // Krill format data structures - collect all data from all ZIPs before output
    data class KrillTextData(
        var textId: String,
        var textContent: String? = null,
        var headerMetadata: MutableMap<String, Any> = mutableMapOf(),
        var tokens: Array<Span>? = null,
        var sentences: Array<Span>? = null,
        var morphoByFoundry: MutableMap<String, MutableMap<String, MorphoSpan>> = mutableMapOf(),
        var structureSpans: MutableList<StructureSpan> = mutableListOf(),
        var extractedAttributes: MutableMap<String, String> = mutableMapOf(),
        var  lpSentencesCollected: Boolean = false,
        var sentencesCollectedByFoundry: MutableSet<String> = mutableSetOf(),
        var constituencyCollectedByFoundry: MutableSet<String> = mutableSetOf()
    )

    private val BASE_STRUCTURE_FOUNDRIES = setOf("base", "dereko")

    private val safeDomFactory: DocumentBuilderFactory by lazy {
        DocumentBuilderFactory.newInstance().apply {
            isNamespaceAware = false
            trySetFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)
            trySetFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
            trySetFeature("http://xml.org/sax/features/external-general-entities", false)
            trySetFeature("http://xml.org/sax/features/external-parameter-entities", false)
            try {
                setAttribute("http://javax.xml.XMLConstants/property/accessExternalDTD", "")
            } catch (_: Exception) {}
            try {
                setAttribute("http://javax.xml.XMLConstants/property/accessExternalSchema", "")
            } catch (_: Exception) {}
        }
    }

    private fun DocumentBuilderFactory.trySetFeature(feature: String, enabled: Boolean) {
        try {
            setFeature(feature, enabled)
        } catch (_: Exception) {}
    }

    private fun StructureSpan.splitFoundryAndLayer(): Pair<String, String>? {
        val separatorIdx = layer.indexOf('/')
        if (separatorIdx <= 0 || separatorIdx == layer.length - 1) {
            return null
        }
        val foundry = layer.substring(0, separatorIdx)
        val descriptor = layer.substring(separatorIdx + 1)
        if (foundry.isBlank() || descriptor.isBlank()) {
            return null
        }
        return foundry to descriptor
    }

    private fun Collection<StructureSpan>.foundriesWithSentenceSpans(): Set<String> =
        this.mapNotNull { span ->
            val (foundry, descriptor) = span.splitFoundryAndLayer() ?: return@mapNotNull null
            if (descriptor == "s:s") foundry else null
        }.toSet()

    private fun Collection<StructureSpan>.foundriesWithConstituencySpans(): Set<String> =
        this.mapNotNull { span ->
            val (foundry, descriptor) = span.splitFoundryAndLayer() ?: return@mapNotNull null
            if (descriptor.startsWith("c:")) foundry else null
        }.toSet()

    private fun Collection<StructureSpan>.sentenceCountsByFoundry(): Map<String, Int> {
        val counts = mutableMapOf<String, Int>()
        this.forEach { span ->
            val (foundry, descriptor) = span.splitFoundryAndLayer() ?: return@forEach
            if (descriptor == "s:s") {
                counts[foundry] = counts.getOrDefault(foundry, 0) + 1
            }
        }
        return counts
    }

    data class StructureSpan(
        val layer: String,  // e.g., "base/s:s", "dereko/s:p"
        val from: Int,
        val to: Int,
        val tokenFrom: Int,
        val tokenTo: Int,
        val depth: Int,
        val attributes: Map<String, String> = emptyMap()
    )

    val krillData: ConcurrentHashMap<String, KrillTextData> = ConcurrentHashMap()
    val corpusMetadata: ConcurrentHashMap<String, MutableMap<String, Any>> = ConcurrentHashMap()
    val docMetadata: ConcurrentHashMap<String, MutableMap<String, Any>> = ConcurrentHashMap()
    val expectedFoundries: MutableSet<String> = mutableSetOf("base")
    val processedFoundries: MutableSet<String> = mutableSetOf()
    var krillOutputCount = java.util.concurrent.atomic.AtomicInteger(0)

    // Inventory-based incremental output
    // Per-ZIP inventory: which texts each ZIP should contain
    val zipInventory: ConcurrentHashMap<String, MutableSet<String>> = ConcurrentHashMap()

    // Track which texts have been processed from which ZIPs
    val processedTextsPerZip: ConcurrentHashMap<String, MutableSet<String>> = ConcurrentHashMap()

    // Lock for synchronized output
    val incrementalOutputLock = Any()

    // Progress bar for incremental output
    var incrementalProgressBar: ProgressBar? = null

    // Track which texts have been output to avoid counting duplicates (thread-safe)
    val outputTexts: MutableSet<String> = ConcurrentHashMap.newKeySet()

    // Scheduled executor for periodic scanning
    var incrementalOutputScheduler: java.util.concurrent.ScheduledExecutorService? = null

    // Shutdown flag for writer thread
    @Volatile var shutdownIncrementalWriter = false

    // Flag to track if TAR stream is still open
    @Volatile var tarStreamOpen = true

    // Lock to synchronize TAR stream access between scanner and main thread
    private val tarStreamLock = java.util.concurrent.locks.ReentrantLock()

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
        // Reset Krill state for fresh run (important for tests)
        if (outputFormat == OutputFormat.KRILL) {
            expectedFoundries.clear()
            expectedFoundries.add("base")
            processedFoundries.clear()
            krillOutputCount.set(0)
            krillData.clear()
            corpusMetadata.clear()
            docMetadata.clear()
            zipInventory.clear()
            processedTextsPerZip.clear()
            outputTexts.clear()
        }

        // Initialize priority-based entry executor
        // Tasks are scheduled based on text ID - foundry with smallest text ID gets priority
        val priorityQueue = java.util.concurrent.PriorityBlockingQueue<Runnable>(
            11,  // initial capacity
            Comparator { r1, r2 ->
                when {
                    r1 is PrioritizedTask && r2 is PrioritizedTask -> r1.compareTo(r2)
                    r1 is PrioritizedTask -> -1  // Prioritized tasks go first
                    r2 is PrioritizedTask -> 1
                    else -> 0  // Equal priority for non-prioritized tasks
                }
            }
        )
        entryExecutor = java.util.concurrent.ThreadPoolExecutor(
            maxThreads,
            maxThreads,
            0L,
            java.util.concurrent.TimeUnit.MILLISECONDS,
            priorityQueue
        )
        LOGGER.info("Initialized watermark-based entry executor with $maxThreads threads (foundries scheduled by text ID to progress together)")

        // Initialize TAR output for krill format
        if (outputFormat == OutputFormat.KRILL) {
            // Find the base ZIP (one without a foundry suffix)
            val baseZip = args.firstOrNull { zip ->
                val name = File(zip).name
                name.matches(Regex(".*\\.zip$")) && !name.matches(Regex(".*\\.[^/.]+\\.zip$"))
            } ?: args[0]

            val baseZipName = File(baseZip).name.replace(Regex("\\.zip$"), "")
            krillOutputFileName = File(outputDir, "$baseZipName.krill.tar").absolutePath
            LOGGER.info("Initializing krill TAR output: $krillOutputFileName")

            if (File(krillOutputFileName!!).exists() && !overwrite) {
                LOGGER.severe("Output file $krillOutputFileName already exists. Use --overwrite to overwrite.")
                exitProcess(1)
            }

            if (File(krillOutputFileName!!).exists()) {
                LOGGER.info("Deleting existing file: $krillOutputFileName")
                File(krillOutputFileName!!).delete()
            }

            val fileOutputStream = FileOutputStream(krillOutputFileName!!)
            krillTarOutputStream = TarArchiveOutputStream(fileOutputStream)
            tarStreamOpen = true  // Stream is now open
            LOGGER.info("Initialized krill TAR output stream")

            // Extract expected foundries from input ZIP filenames for incremental output
            args.forEach { zipPath ->
                val zipName = File(zipPath).name
                // Match pattern: name.foundry.zip (e.g., corpus.spacy.zip)
                val foundryMatch = Regex("^(.+?)\\.([^.]+)\\.zip$").find(zipName)
                if (foundryMatch != null) {
                    val foundry = foundryMatch.groupValues[2]
                    // Handle compound foundries like "marmot-malt" which contains both "marmot" and "malt"
                    if (foundry.contains("-")) {
                        foundry.split("-").forEach { subFoundry ->
                            expectedFoundries.add(subFoundry)
                            LOGGER.info("Expecting foundry: $subFoundry from $zipName")
                        }
                    } else {
                        expectedFoundries.add(foundry)
                        LOGGER.info("Expecting foundry: $foundry from $zipName")
                    }
                } else if (zipName.endsWith(".zip")) {
                    // Base ZIP without foundry suffix
                    LOGGER.info("Base ZIP detected: $zipName")
                }
            }
            LOGGER.info("Expected foundries for Krill output: ${expectedFoundries.sorted()}")

            // Build inventory of which texts exist in which ZIPs for incremental output
            buildZipInventory(args)

            // Initialize progress bar for incremental output
            if (!quiet) {
                val totalTexts = zipInventory.values.flatten().toSet().size
                if (totalTexts > 0) {
                    incrementalProgressBar = ProgressBarBuilder()
                        .setTaskName("$baseZipName.krill.tar")
                        .setInitialMax(totalTexts.toLong())
                        .setStyle(ProgressBarStyle.COLORFUL_UNICODE_BAR)
                        .setUpdateIntervalMillis(500)
                        .showSpeed()
                        .build()
                }
            }

            // Start dedicated writer thread for incremental output
            // Only enable if we have multiple texts to benefit from incremental processing
            val totalTexts = zipInventory.values.flatten().toSet().size
            if (totalTexts > 1) {
                startIncrementalWriterThread()
                LOGGER.info("Enabled incremental output for $totalTexts texts")
            } else {
                LOGGER.info("Disabled incremental output (only $totalTexts text)")
            }
        }

        if (annotateWith.isNotEmpty()) {
            // Detect external foundry label once from annotateWith command
            externalFoundry = detectFoundryFromAnnotateCmd(annotateWith)
            // Initialize ZIP output stream BEFORE creating worker pool, if needed
            if (outputFormat == OutputFormat.KORAPXML) {
                // Determine output filename
                val inputZipPath = args[0] // First ZIP file
                val targetFoundry = externalFoundry ?: "annotated"

                val outputMorphoZipFileName = inputZipPath.replace(Regex("\\.zip$"), ".".plus(targetFoundry).plus(".zip"))
                targetZipFileName = outputMorphoZipFileName
                LOGGER.info("Initializing output ZIP: $outputMorphoZipFileName (from input: $inputZipPath, foundry: $targetFoundry)")
                // Prepare per-output log file
                val logFilePath = outputMorphoZipFileName.replace(Regex("\\.zip$"), ".log")
                val fileHandler = java.util.logging.FileHandler(logFilePath, true)
                fileHandler.formatter = ColoredFormatter()
                LOGGER.addHandler(fileHandler)
                LOGGER.info("Logging redirected to: $logFilePath")
                // Mirror System.err to the same log file for the duration
                val errPs = java.io.PrintStream(java.io.BufferedOutputStream(java.io.FileOutputStream(logFilePath, true)), true)
                val oldErr = System.err
                System.setErr(errPs)

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

                // Ensure we restore System.err and remove file handler at the end of processing (shutdown hook)
                Runtime.getRuntime().addShutdownHook(Thread {
                    try {
                        LOGGER.info("Shutting down; closing per-zip log handler")
                        LOGGER.removeHandler(fileHandler)
                        fileHandler.close()
                    } catch (_: Exception) {}
                    try { System.setErr(oldErr) } catch (_: Exception) {}
                    try { errPs.close() } catch (_: Exception) {}
                })
            }

            if (outputFormat == OutputFormat.KORAPXML) {
                // For ZIP output with external annotation, we need a custom handler
                val currentZipPath = args[0].replace(Regex("\\.zip$"), "." + (externalFoundry ?: "annotated") + ".zip")
                val currentLog = currentZipPath.replace(Regex("\\.zip$"), ".log")
                annotationWorkerPool = AnnotationWorkerPool(annotateWith, maxThreads, LOGGER, { annotatedConllu, task ->
                     parseAndWriteAnnotatedConllu(annotatedConllu, task)
                }, stderrLogPath = currentLog)
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
                    LOGGER.warning("Entry executor did not terminate within timeout")
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
        } else {
            // No external worker: ensure progress bar is closed (e.g., internal tagger -t)
            progressBar?.close()
        }
        // Shutdown entry executor
        entryExecutor?.shutdown()

        // Stop incremental writer thread if running
        stopIncrementalWriterThread()
        // Keep incrementalProgressBar open - continue using it for remaining texts

        // Finalize krill output: output any remaining incomplete texts and close TAR
        if (outputFormat == OutputFormat.KRILL && krillTarOutputStream != null) {
            try {
                val remainingCount = krillData.size
                if (remainingCount > 0) {
                    LOGGER.info("Outputting $remainingCount remaining incomplete texts (already output: $krillOutputCount)")
                } else {
                    LOGGER.info("Outputting remaining texts ($krillOutputCount already output incrementally)")
                }

                // Continue using the same progress bar for remaining texts (no separate bar)
                // Output remaining texts (these weren't output incrementally, possibly incomplete)
                // Copy keys to avoid ConcurrentModificationException if scanner is still running
                val remainingKeys = krillData.keys.sorted()
                remainingKeys.forEach { textId ->
                    val textData = krillData.remove(textId) ?: return@forEach  // Skip if already removed by scanner
                    val textFoundries = textData.morphoByFoundry.keys.toSet() + setOf("base")

                    // Build expected foundries from inventory: which ZIPs contain this text?
                    val expectedForThisText = zipInventory.filter { (_, texts) -> texts.contains(textId) }.keys
                        .flatMap { zipPath ->
                            val foundry = getFoundryFromZipFileName(File(zipPath).name)
                            if (foundry.contains("-")) foundry.split("-") else listOf(foundry)
                        }
                        .toSet()

                    if (!textFoundries.containsAll(expectedForThisText)) {
                        LOGGER.warning("Outputting incomplete text $textId with foundries ${textFoundries.sorted()} (expected: ${expectedForThisText.sorted()})")
                    }
                    outputKrillText(textId, textData)
                    // Continue stepping the same progress bar
                    incrementalProgressBar?.step()
                }

                // Acquire lock before closing stream to prevent concurrent scanner access
                tarStreamLock.lock()
                try {
                    // Set flag before closing stream to prevent scanner from trying to write
                    tarStreamOpen = false

                    krillTarOutputStream!!.finish()
                    krillTarOutputStream!!.close()
                } finally {
                    tarStreamLock.unlock()
                }

                // Close incremental progress bar if it was initialized
                incrementalProgressBar?.close()

                LOGGER.info("Closed krill TAR file: $krillOutputFileName (total texts output: $krillOutputCount)")
            } catch (e: Exception) {
                LOGGER.severe("ERROR generating krill output: ${e.message}")
                e.printStackTrace()
            }
        }
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
                        // For krill format, use per-ZIP foundry; otherwise use shared foundry
                        val zipFoundry = if (outputFormat == OutputFormat.KRILL) {
                            getFoundryFromZipFileName(zipPath)
                        } else {
                            foundry
                        }
                        LOGGER.info("Processing ZIP: $zipPath with foundry=$zipFoundry")
                        if (sequentialInZip) {
                            processZipFileSequentially(zipPath, zipFoundry)
                        } else {
                            processZipFile(zipPath, zipFoundry)
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

    private fun NodeList.asElementSequence(): Sequence<Element> = sequence {
        for (i in 0 until length) {
            val node = item(i)
            if (node is Element) {
                yield(node)
            }
        }
    }

    private fun Element?.childElements(tagName: String): Sequence<Element> =
        this?.getElementsByTagName(tagName)?.asElementSequence() ?: emptySequence()

    private fun Element?.firstElement(tagName: String, predicate: (Element) -> Boolean = { true }): Element? =
        childElements(tagName).firstOrNull(predicate)

    private fun Element?.firstText(tagName: String, predicate: (Element) -> Boolean = { true }): String? =
        firstElement(tagName, predicate)?.textContent?.trim()?.takeIf { it.isNotEmpty() }

    private fun Element?.collectTexts(tagName: String, predicate: (Element) -> Boolean = { true }): List<String> =
        childElements(tagName)
            .filter(predicate)
            .mapNotNull { el -> el.textContent?.trim()?.takeIf { it.isNotEmpty() } }
            .toList()

    private fun MutableMap<String, Any>.putIfNotBlank(key: String, value: String?) {
        val trimmed = value?.trim()
        if (!trimmed.isNullOrEmpty()) {
            this[key] = trimmed
        }
    }

    private fun Element?.collectCatRefTopics(): List<String> {
        if (this == null) return emptyList()
        val topics = mutableListOf<String>()
        val catRefs = this.getElementsByTagName("catRef")
        for (i in 0 until catRefs.length) {
            val element = catRefs.item(i) as? Element ?: continue
            val target = element.getAttribute("target")
            if (!target.isNullOrBlank()) {
                topics.addAll(target.split('.').drop(1).filter { it.isNotBlank() })
            }
        }
        return topics
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
            // Determine output zip label. Prefer combined label if both tagger and parser are active
            var targetFoundry = "base"
            val labelParts = mutableListOf<String>()
            if (taggerName != null) {
                val tagger = AnnotationToolBridgeFactory.getTagger(taggerName!!, taggerModel!!, LOGGER)
                if (tagger != null) {
                    labelParts.add(tagger.foundry)
                }
            }
            if (parserName != null) {
                labelParts.add(parserName!!)
            }
            if (labelParts.isNotEmpty()) {
                targetFoundry = labelParts.joinToString("-")
            } else if (annotateWith.isNotEmpty()) {
                targetFoundry = externalFoundry ?: detectFoundryFromAnnotateCmd(annotateWith)
                LOGGER.info("Detected foundry '$targetFoundry' from annotation command: $annotateWith")
            }
            dbFactory = DocumentBuilderFactory.newInstance()
            dBuilder = dbFactory!!.newDocumentBuilder()
            val outputMorphoZipFileName = zipFilePath.replace(Regex("\\.zip$"), "." + targetFoundry + ".zip")
            targetZipFileName = outputMorphoZipFileName
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
        LOGGER.fine("About to process ZIP entries: hasCorrespondingBaseZip=${zipFilePath.hasCorrespondingBaseZip()}")
        if (zipFilePath.hasCorrespondingBaseZip()) {
            val relatedZips = arrayOf(zipFilePath, zipFilePath.correspondingBaseZip()!!)
            // Process related zips one after another to keep the ZipFile lifetime strictly bounded
            relatedZips.forEach { zip ->
                // For krill format, use per-ZIP foundry; for other formats, use the original foundry
                val zipFoundry = if (outputFormat == OutputFormat.KRILL) {
                    if (zip == zipFilePath.correspondingBaseZip()) "base" else foundry
                } else {
                    foundry  // Keep original foundry for non-krill formats
                }
                ApacheZipFile(File(zip)).use { zipFile ->
                    processZipEntriesWithPool(zipFile, zip, zipFoundry, true)
                }
            }
        } else {
            LOGGER.fine("Opening ZipFile for processing: $zipFilePath")
            try {
                // If no corresponding base ZIP exists, this IS the base ZIP
                ApacheZipFile(File(zipFilePath)).use { zipFile ->
                    LOGGER.fine("Calling processZipEntriesWithPool, foundry=$foundry")
                    processZipEntriesWithPool(zipFile, zipFilePath, foundry, false)
                    LOGGER.fine("Returned from processZipEntriesWithPool")
                }
            } catch (e: Exception) {
                LOGGER.severe("Error processing ZIP: ${e.message}")
                e.printStackTrace()
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

        // Track foundry as processed
        if (outputFormat == OutputFormat.KRILL) {
            processedFoundries.add(foundry)
        }
    }

    private fun processZipFileSequentially(zipFilePath: String, foundry: String = "base") {
        val ord = zipOrdinals[zipFilePath] ?: 0
        val size = zipSizes[zipFilePath] ?: 0L
        LOGGER.info("Processing zip ${if (ord>0) ord else "?"}/$totalZips: ${zipFilePath} (${humanBytes(size)}) in thread ${Thread.currentThread().threadId()}")
        if (zipFilePath.hasCorrespondingBaseZip()) {
            // Process the two related zips strictly sequentially to limit memory growth
            val zips = arrayOf(zipFilePath, zipFilePath.correspondingBaseZip()!!)
            zips.forEach { zip ->
                // For krill format, use per-ZIP foundry; for other formats, use the original foundry
                val zipFoundry = if (outputFormat == OutputFormat.KRILL) {
                    if (zip == zipFilePath.correspondingBaseZip()) "base" else foundry
                } else {
                    foundry  // Keep original foundry for non-krill formats
                }
                ApacheZipFile(File(zip)).use { zipFile ->
                    // Iterate entries sorted by text ID to ensure consistent processing order
                    zipFile.entries.toList()
                        .filter { extractMetadataRegex.isNotEmpty() || !it.name.contains("header.xml") }
                        .sortedBy { getTextIdFromPath(it.name) }
                        .forEach { zipEntry ->
                            processZipEntry(zipFile, zip, zipFoundry, zipEntry, true)
                        }
                }
            }
        } else {
            ApacheZipFile(File(zipFilePath)).use { zipFile ->
                zipFile.entries.toList()
                    .filter { extractMetadataRegex.isNotEmpty() || !it.name.contains("header.xml") }
                    .sortedBy { getTextIdFromPath(it.name) }
                    .forEach { zipEntry ->
                        processZipEntry(zipFile, zipFilePath, foundry, zipEntry, false)
                    }
            }
        }
        logZipProgress(zipFilePath)

        // Track foundry as processed
        if (outputFormat == OutputFormat.KRILL) {
            processedFoundries.add(foundry)
        }
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

    private fun processZipEntriesWithPool(zipFile: ApacheZipFile, zipPath: String, foundry: String, waitForMorpho: Boolean) {
        // Collect entries first to avoid lazy evaluation surprises, filter header.xml unless metadata extraction is requested
        val entries: MutableList<ZipArchiveEntry> = ArrayList()
        var documentCount = 0
        val enumEntries = zipFile.entries
        while (enumEntries.hasMoreElements()) {
            val e = enumEntries.nextElement()
            // Skip header.xml unless metadata extraction is requested OR output format is KRILL
            if (extractMetadataRegex.isEmpty() && outputFormat != OutputFormat.KRILL && e.name.contains("header.xml")) continue
            entries.add(e)
        }
        LOGGER.fine("Collected ${entries.size} entries from ZIP, foundry=$foundry")
        if (entries.isEmpty()) return

        // Sort entries by text ID to ensure texts complete as early as possible
        // This is crucial for incremental output - all ZIPs will process texts in the same order
        entries.sortBy { entry ->
            // Extract text ID from path like "ZGE24/JAN/00001/base/data.xml" -> "ZGE24_JAN.00001"
            getTextIdFromPath(entry.name)
        }
        LOGGER.fine("Sorted entries by text ID for incremental processing")

        // Determine document count for progress: prefer data.xml, fallback to tokens.xml
        documentCount = entries.count { it.name.contains("data.xml") }
        if (documentCount == 0) {
            documentCount = entries.count { it.name.contains("tokens.xml") }
        }

        // Update total document count and start timer if this is the first ZIP with external annotation
        // Initialize progress bar either for external annotation (-A) or internal tagging (-t)
        if ((annotationWorkerPool != null || taggerName != null) && documentCount > 0) {
             val newTotal = totalDocsInInput.addAndGet(documentCount)
             if (annotationStartTime.get() == 0L) {
                annotationStartTime.set(System.currentTimeMillis())
                LOGGER.info("Starting annotation of $newTotal document(s)")
                if (!quiet) {
                     // Initialize progress bar for external annotation with ZIP output
                     progressBar = ProgressBarBuilder()
                         .setTaskName(targetZipFileName ?: "Annotating")
                         .setInitialMax(newTotal.toLong())
                         .setStyle(ProgressBarStyle.COLORFUL_UNICODE_BAR)
                         .setUpdateIntervalMillis(500)
                         .showSpeed()
                         .build()
                }
            } else if (!quiet) {
                // Increase the total as we discover more documents in later zips
                progressBar?.maxHint(newTotal.toLong())
            }
         }

        // If only one thread requested, do sequential to avoid pool overhead
        if (maxThreads <= 1) {
            entries.forEach { entry -> processZipEntry(zipFile, zipPath, foundry, entry, waitForMorpho) }
            return
        }

        // Group entries by text ID to ensure all files for a text are processed together
        val entriesByTextId = entries.groupBy { getTextIdFromPath(it.name) }
        val textIds = entriesByTextId.keys.sorted()  // Process text IDs in lexicographic order

        // Initialize watermark for this foundry if not exists (set to first text ID)
        if (!foundryWatermarks.containsKey(foundry) && textIds.isNotEmpty()) {
            foundryWatermarks.putIfAbsent(foundry, textIds.first())
            LOGGER.fine("Initialized watermark for $foundry to ${textIds.first()}")
        }

        // Log current foundry watermarks for debugging
        val watermarkStats = foundryWatermarks.entries.sortedBy { it.key }.joinToString(", ") { entry ->
            "${entry.key}=${entry.value}"
        }
        if (watermarkStats.isNotEmpty()) {
            LOGGER.fine("Foundry watermarks before submitting $foundry entries: $watermarkStats")
        }

        // Submit one task per text ID (each task processes all entries for that text)
        val latch = java.util.concurrent.CountDownLatch(textIds.size)
        textIds.forEach { textId ->
            val textEntries = entriesByTextId[textId] ?: emptyList()

            val prioritizedTask = PrioritizedTask(foundry, textId, Runnable {
                try {
                    // Process all entries for this text ID sequentially
                    textEntries.forEach { entry ->
                        processZipEntry(zipFile, zipPath, foundry, entry, waitForMorpho)
                    }

                    // Update watermark after completing this text
                    foundryWatermarks[foundry] = textId
                } catch (t: Throwable) {
                    LOGGER.warning("Failed to process text $textId: ${t.message}")
                } finally {
                    latch.countDown()
                }
            })
            entryExecutor?.execute(prioritizedTask)
        }
        try {
            latch.await()
        } catch (ie: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    fun processZipEntry(zipFile: ApacheZipFile, zipPath: String, _foundry: String, zipEntry: ZipArchiveEntry, passedWaitForMorpho: Boolean) {
        var foundry = _foundry
        var waitForMorpho = passedWaitForMorpho
        LOGGER.finer("Processing ${zipEntry.name} in thread ${Thread.currentThread().threadId()}")
        if (taggerName != null && !taggerToolBridges.containsKey(Thread.currentThread().threadId())) {
            val tagger = AnnotationToolBridgeFactory.getTagger(taggerName!!, taggerModel!!, LOGGER)
            if (tagger != null) {
                taggerToolBridges[Thread.currentThread().threadId()] = tagger
                foundry = tagger.foundry
            }

        }
        if (parserName != null && !parserToolBridges.containsKey(Thread.currentThread().threadId()) && !constituencyParserBridges.containsKey(Thread.currentThread().threadId())) {
            // If both tagger and parser are CoreNLP, pass tagger model to parser for POS tagging
            val taggerModelForParser = if (parserName == "corenlp" && taggerName == "corenlp") taggerModel else null
            val parser = AnnotationToolBridgeFactory.getParser(parserName!!, parserModel!!, LOGGER, taggerModelForParser)
            when (parser) {
                is ParserToolBridge -> {
                    parserToolBridges[Thread.currentThread().threadId()] = parser
                    foundry = "$foundry dependency:${parser.foundry}"
                    LOGGER.fine("Initialized dependency parser ${parserName} with foundry $foundry in thread ${Thread.currentThread().threadId()}")
                }
                is ConstituencyParserBridge -> {
                    constituencyParserBridges[Thread.currentThread().threadId()] = parser
                    foundry = "${parser.foundry}"
                    LOGGER.fine("Initialized constituency parser ${parserName} with foundry $foundry in thread ${Thread.currentThread().threadId()}")
                }
                else -> {
                    LOGGER.warning("Parser ${parserName} returned null or unknown type")
                }
            }
        }

        // Ensure foundry reflects active tagger/parser even if already initialized earlier on this thread
        taggerToolBridges[Thread.currentThread().threadId()]?.let { activeTagger ->
            foundry = activeTagger.foundry
        }
        parserToolBridges[Thread.currentThread().threadId()]?.let { activeParser ->
            foundry = "$foundry dependency:${activeParser.foundry}"
        }
        constituencyParserBridges[Thread.currentThread().threadId()]?.let { activeParser ->
            foundry = "${activeParser.foundry}"
        }

        try {
            if (zipEntry.name.matches(Regex(".*(data|tokens|structure|morpho|dependency|sentences|constituency)\\.xml$"))) {
                LOGGER.finer("Processing entry: ${zipEntry.name}, foundry=$foundry")
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

                        // For krill format, collect structural spans and base data (only from base foundry to avoid duplicates)
                        if (outputFormat == OutputFormat.KRILL && foundry == "base") {
                            collectKrillStructureSpans(docId, spans)
                            // Also collect base data here to ensure sentences are included
                            // (handles race condition where structure.xml is processed after tokens.xml)
                            collectKrillBaseData(docId)
                        }
                    }

                    "tokens.xml" -> {
                        if (!fnames.contains(docId)) {
                            fnames[docId] = zipEntry.name
                        }
                        val tokenSpans: NodeList = doc.getElementsByTagName("span")
                        tokens[docId] = extractSpans(tokenSpans, docId)

                        // For krill format with base foundry, collect base text data immediately
                        if (outputFormat == OutputFormat.KRILL && foundry == "base") {
                            collectKrillBaseData(docId)
                        }
                    }

                    "morpho.xml" -> {
                        waitForMorpho = true
                        fnames[docId] = zipEntry.name
                        LOGGER.info("Processing morpho.xml for $docId with foundry=$foundry from ${zipEntry.name}")
                        val fsSpans: NodeList = doc.getElementsByTagName("span")
                        val morphoSpans = extractMorphoSpans(fsSpans)

                        // For krill format, collect morpho data directly without using shared morpho map
                        if (outputFormat == OutputFormat.KRILL) {
                            val morphoFoundry = getFoundryForLayer(foundry, "morpho")
                            collectKrillMorphoDataDirect(docId, morphoFoundry, morphoSpans, "morpho")
                            tokens[docId] = extractSpans(fsSpans, docId)
                        } else {
                            // For other formats, use the shared morpho map
                            // Merge with existing morpho data (e.g., from dependency.xml)
                            // Synchronize access to morpho[docId] to avoid race conditions
                            val morphoMap = synchronized(morpho) {
                                morpho.getOrPut(docId) { morphoSpans }
                            }

                            if (morphoMap !== morphoSpans) {
                                // Map already existed, need to merge
                                synchronized(morphoMap) {
                                    morphoSpans.forEach { (key, mfs) ->
                                        val existing = morphoMap[key]
                                        if (existing != null) {
                                            // Preserve head and deprel from existing (dependency.xml)
                                            mfs.head = existing.head
                                            mfs.deprel = existing.deprel
                                        }
                                        morphoMap[key] = mfs
                                    }
                                    LOGGER.fine("Merged morpho.xml with existing data for $docId (preserved ${morphoMap.count { it.value.head != "_" }} dependency relations)")
                                }
                            }
                            tokens[docId] = extractSpans(fsSpans, docId)
                        }
                    }

                    "dependency.xml" -> {
                        LOGGER.info("Processing dependency.xml for $docId from ${zipEntry.name}")
                        val depSpans: NodeList = doc.getElementsByTagName("span")
                        LOGGER.info("Found ${depSpans.length} spans in dependency.xml")
                        val depMap = extractDependencySpans(depSpans)
                        LOGGER.info("Extracted ${depMap.size} dependency relations")

                        // For krill format, collect dependency data directly without using shared morpho map
                        if (outputFormat == OutputFormat.KRILL) {
                            val depFoundry = getFoundryForLayer(foundry, "dependency")
                            collectKrillMorphoDataDirect(docId, depFoundry, depMap, "dependency")
                        } else {
                            // For other formats, merge dependency info into existing morpho data
                            // Note: heads are stored as offsets (e.g., "100-110") and will be resolved
                            // to token indices later during CoNLL-U output
                            // Synchronize access to morpho[docId] to avoid race conditions
                            val morphoMap = synchronized(morpho) {
                                morpho.getOrPut(docId) {
                                    LOGGER.info("Created new morpho map for $docId")
                                    mutableMapOf()
                                }
                            }

                            var mergedCount = 0
                            var newCount = 0
                            synchronized(morphoMap) {
                                depMap.forEach { (key, depSpan) ->
                                    val existing = morphoMap[key]
                                    if (existing != null) {
                                        // Update existing morpho with dependency info (head is still offset-based)
                                        existing.head = depSpan.head
                                        existing.deprel = depSpan.deprel
                                        mergedCount++
                                    } else {
                                        // Create new entry with just dependency info
                                        morphoMap[key] = depSpan
                                        newCount++
                                    }
                                }
                            }
                            LOGGER.info("Dependency merge complete: $mergedCount merged, $newCount new entries (heads will be resolved during output)")
                        }
                    }

                    "sentences.xml" -> {
                        LOGGER.fine("Sentences entry foundry=$foundry for $docId from ${zipEntry.name}")
                        if (outputFormat == OutputFormat.KRILL) {
                            val sentenceSpans: NodeList = doc.getElementsByTagName("span")
                            collectSentences(docId, foundry, sentenceSpans)
                        }
                    }

                    "constituency.xml" -> {
                        if (outputFormat == OutputFormat.KRILL) {
                            val constituencySpans: NodeList = doc.getElementsByTagName("span")
                            collectConstituency(docId, foundry, constituencySpans)
                        }
                    }
                }

                // Mark text as processed from this ZIP for incremental output
                if (outputFormat == OutputFormat.KRILL) {
                    // Mark this text as processed from this ZIP (writer thread will scan periodically)
                    processedTextsPerZip.getOrPut(zipPath) { mutableSetOf() }.add(docId)
                }

                val morphoRequired = when {
                    // If tagger or parser is enabled, we generate annotations in processText
                    taggerName != null || parserName != null -> false
                    // Word2Vec/NOW with lemmas needs morpho unless a tagger is active (handled above)
                    useLemma -> true
                    // Explicit wait flag means: require morpho.xml before proceeding
                    waitForMorpho -> true
                    // For direct KorAPXML output without external annotator, require morpho unless -t/-P (handled above)
                    outputFormat == OutputFormat.KORAPXML && annotationWorkerPool == null -> true
                    // For krill format, morpho is not required - we collect whatever is available
                    outputFormat == OutputFormat.KRILL -> false
                    else -> false
                }
                // For lemma-only/lemma-based word2vec/now, we can proceed without full text
                val textRequired = when (outputFormat) {
                    OutputFormat.WORD2VEC, OutputFormat.NOW -> !(useLemma || lemmaOnly)
                    OutputFormat.KRILL -> true  // Krill needs text from base ZIP
                    else -> true
                }

                LOGGER.fine("Checking if ready to process $docId: texts=${texts[docId] != null}, sentences=${sentences[docId] != null}, tokens=${tokens[docId] != null}, morpho=${morpho[docId] != null}, morphoRequired=$morphoRequired, textRequired=$textRequired")

                if ((texts[docId] != null || !textRequired) && sentences[docId] != null && tokens[docId] != null
                    && (!morphoRequired || morpho[docId] != null)
                    && (extractMetadataRegex.isEmpty() || metadata[docId] != null)
                ) {
                    LOGGER.fine("All data ready for $docId, calling processText")
                    processText(docId, foundry)
                } else {
                    LOGGER.fine("NOT ready to process $docId yet: textOK=${texts[docId] != null || !textRequired}, sentencesOK=${sentences[docId] != null}, tokensOK=${tokens[docId] != null}, morphoOK=${!morphoRequired || morpho[docId] != null}")
                }
            } else if ((extractMetadataRegex.isNotEmpty() || outputFormat == OutputFormat.KRILL) && zipEntry.name.matches(Regex(".*/header\\.xml$"))) {
                val headerBytes = zipFile.getInputStream(zipEntry).use { it.readBytes() }
                val text = headerBytes.toString(Charsets.UTF_8)
                val headerDoc = safeDomFactory.newDocumentBuilder().parse(ByteArrayInputStream(headerBytes))
                val headerRoot = headerDoc.documentElement
                headerRoot.normalize()

                val textSigle = headerRoot.firstText("textSigle")
                val docSigle = headerRoot.firstText("dokumentSigle")
                val corpusSigle = headerRoot.firstText("korpusSigle")

                val docId = textSigle?.replace('/', '_')
                LOGGER.fine("Processing header file: " + zipEntry.name + " docId: " + docId + " corpusSigle: " + corpusSigle + " docSigle: " + docSigle)

                if (outputFormat == OutputFormat.KRILL) {
                    when {
                        corpusSigle != null -> collectCorpusMetadata(corpusSigle, headerRoot)
                        docSigle != null -> collectDocMetadata(docSigle, headerRoot)
                        docId != null -> collectKrillMetadata(docId, headerRoot)
                    }
                }

                val meta = ArrayList<String>()
                extractMetadataRegex.forEach { regex ->
                    val match = Regex(regex).find(text)
                    if (match != null) {
                        meta.add(match.destructured.component1())
                    }
                }
                if (meta.isNotEmpty() && docId != null) {
                    metadata[docId] = meta.toTypedArray()
                    val morphoRequired = when {
                        taggerName != null || parserName != null -> false
                        useLemma -> true
                        waitForMorpho -> true
                        outputFormat == OutputFormat.KORAPXML && annotationWorkerPool == null -> true
                        outputFormat == OutputFormat.KRILL -> false
                        else -> false
                    }
                    val textRequired = when (outputFormat) {
                        OutputFormat.WORD2VEC, OutputFormat.NOW -> !(useLemma || lemmaOnly)
                        OutputFormat.KRILL -> true
                        else -> true
                    }
                    if ((texts[docId] != null || !textRequired) && sentences[docId] != null && tokens[docId] != null
                        && (!morphoRequired || morpho[docId] != null)
                    ) {
                        LOGGER.info("Processing text (meta-ready): $docId in thread ${Thread.currentThread().threadId()}")
                        processText(docId, foundry)
                    }
                }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    private fun detectFoundryFromAnnotateCmd(cmd: String): String {
        val lower = cmd.lowercase(Locale.getDefault())
        return when {
            lower.contains("spacy") -> "spacy"
            lower.contains("stanza") -> "stanza"
            lower.contains("udpipe") -> "udpipe"
            lower.contains("tree") -> "tree_tagger"
            lower.contains("marmot") -> "marmot"
            lower.contains("opennlp") -> "opennlp"
            lower.contains("corenlp") -> "corenlp"
            else -> "annotated"
        }
    }

    private fun processText(
        docId: String,
        foundry: String,
    ) {
        LOGGER.fine("processText called: $docId, foundry=$foundry, outputFormat=$outputFormat")
        var morphoFoundry = getMorphoFoundry()

        // Special handling for krill format: data is collected immediately when parsed, not here
        if (outputFormat == OutputFormat.KRILL) {
            return
        }

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
            if (constituencyParserBridges[Thread.currentThread().threadId()] != null) {
                LOGGER.finer("Constituency parsing text: $docId in thread ${Thread.currentThread().threadId()}")
                val trees = constituencyParserBridges[Thread.currentThread().threadId()]!!.parseConstituency(
                    tokens[docId]!!,
                    morpho[docId],
                    sentences[docId],
                    texts[docId]!!
                )
                constituencyTrees[docId] = trees
                LOGGER.finer("Constituency parsed text: $docId, generated ${trees.size} trees in thread ${Thread.currentThread().threadId()}")
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
                // Use external foundry label for folder names when using --annotate-with
                val targetFoundry = externalFoundry
                    ?: taggerToolBridges[Thread.currentThread().threadId()]?.foundry
                    ?: (parserName ?: morphoFoundry)
                val entryPath = if (parserName != null)
                    docId.replace(Regex("[_.]"), "/") + "/$targetFoundry/dependency.xml"
                else
                    docId.replace(Regex("[_.]"), "/") + "/$targetFoundry/morpho.xml"
                 LOGGER.fine("Sending document $docId (${output.length} chars) to annotation worker pool for ZIP output")
                 // Pass metadata via AnnotationTask, NOT in the text itself
                annotationWorkerPool?.pushToQueue(output.toString(), docId, entryPath + "|" + targetFoundry)
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
            // Direct ZIP output without external annotation: write morpho.xml and, if parser is active, dependency.xml
            val morphoDir = taggerToolBridges[Thread.currentThread().threadId()]?.foundry ?: morphoFoundry
            val depDir = parserName ?: morphoDir
             var wroteOne = false
             // Always write morpho.xml if we have morpho annotations (tagger or from input)
             if (morpho[docId] != null && morpho[docId]!!.isNotEmpty()) {
                val morphoXml = korapXmlMorphoOutput(morphoDir, docId).toString()
                val morphoPath = docId.replace(Regex("[_.]"), "/") + "/$morphoDir/morpho.xml"
                 val morphoEntry = ZipArchiveEntry(morphoPath)
                 morphoEntry.unixMode = ZIP_ENTRY_UNIX_MODE
                 synchronized(morphoZipOutputStream!!) {
                     morphoZipOutputStream!!.putArchiveEntry(morphoEntry)
                     morphoZipOutputStream!!.write(morphoXml.toByteArray())
                     morphoZipOutputStream!!.closeArchiveEntry()
                 }
                 wroteOne = true
             }
             // Write dependency.xml if a parser is active and dependency info present
             if (parserToolBridges[Thread.currentThread().threadId()] != null) {
                val depXml = korapXmlDependencyOutput(depDir, docId).toString()
                val depPath = docId.replace(Regex("[_.]"), "/") + "/$depDir/dependency.xml"
                 val depEntry = ZipArchiveEntry(depPath)
                 depEntry.unixMode = ZIP_ENTRY_UNIX_MODE
                 synchronized(morphoZipOutputStream!!) {
                     morphoZipOutputStream!!.putArchiveEntry(depEntry)
                     morphoZipOutputStream!!.write(depXml.toByteArray())
                     morphoZipOutputStream!!.closeArchiveEntry()
                 }
                 wroteOne = true
             }
             // Write constituency.xml if a constituency parser is active
             if (constituencyParserBridges[Thread.currentThread().threadId()] != null && constituencyTrees[docId] != null) {
                val constDir = constituencyParserBridges[Thread.currentThread().threadId()]!!.foundry
                val constXml = korapXmlConstituencyOutput(constDir, docId).toString()
                val constPath = docId.replace(Regex("[_.]"), "/") + "/$constDir/constituency.xml"
                 val constEntry = ZipArchiveEntry(constPath)
                 constEntry.unixMode = ZIP_ENTRY_UNIX_MODE
                 synchronized(morphoZipOutputStream!!) {
                     morphoZipOutputStream!!.putArchiveEntry(constEntry)
                     morphoZipOutputStream!!.write(constXml.toByteArray())
                     morphoZipOutputStream!!.closeArchiveEntry()
                 }
                 wroteOne = true
             }
             output.clear()
             // Track written docs once per document and update progress like with --annotate-with
             val written = if (wroteOne) docsWrittenToZip.incrementAndGet() else docsWrittenToZip.get()
             if (!quiet) progressBar?.step()
             if (totalDocsInInput.get() > 0) {
                 val total = totalDocsInInput.get()
                 val percent = (written * 100.0) / total
                 val elapsed = (System.currentTimeMillis() - annotationStartTime.get()) / 1000.0
                 val docsPerSec = if (elapsed > 0) written / elapsed else 0.0
                 val remaining = total - written
                 val etaSec = if (docsPerSec > 0) remaining / docsPerSec else 0.0
                 if (written % 10 == 0 || written == total) {
                     LOGGER.info(String.format(Locale.ROOT,
                         "Progress: %d/%d (%.1f%%), %.1f docs/s, ETA %02d:%02d",
                         written, total, percent, docsPerSec, (etaSec / 60).toInt(), (etaSec % 60).toInt()))
                 }
             }
         }

        // Release per-document data to free memory early
        arrayOf(tokens, texts, sentences, morpho, constituencyTrees, fnames, metadata, extraFeatures).forEach { map ->
            if (map === morpho) {
                morpho[docId]?.clear()
            }
            map.remove(docId)
        }

        // Non-ZIP outputs (-t without -f zip): still advance the bar per processed document
        if (annotationWorkerPool == null && outputFormat != OutputFormat.KORAPXML) {
            if (!quiet) progressBar?.step()
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

    private fun korapXmlConstituencyOutput(foundry: String, docId: String): StringBuilder {
        val doc: Document = dBuilder!!.newDocument()

        // Root element
        val layer = doc.createElement("layer")
        layer.setAttribute("xmlns", "http://ids-mannheim.de/ns/KorAP")
        layer.setAttribute("version", "KorAP-0.4")
        layer.setAttribute("docid", docId)
        doc.appendChild(layer)

        val spanList = doc.createElement("spanList")
        layer.appendChild(spanList)

        val trees = constituencyTrees[docId]
        if (trees == null || trees.isEmpty()) {
            LOGGER.warning("No constituency trees found for $docId")
            return StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
        }

        // Process each tree
        trees.forEach { tree ->
            tree.nodes.forEach { node ->
                // Create span element
                val spanNode = doc.createElement("span")
                spanNode.setAttribute("id", node.id)
                spanNode.setAttribute("from", node.from.toString())
                spanNode.setAttribute("to", node.to.toString())

                // Create fs element for the constituency label
                val fs = doc.createElement("fs")
                fs.setAttribute("xmlns", "http://www.tei-c.org/ns/1.0")
                fs.setAttribute("type", "node")

                val f = doc.createElement("f")
                f.setAttribute("name", "const")
                f.textContent = node.label
                fs.appendChild(f)

                spanNode.appendChild(fs)

                // Add rel elements for children
                node.children.forEach { child ->
                    val rel = doc.createElement("rel")
                    rel.setAttribute("label", "dominates")

                    when (child) {
                        is ConstituencyParserBridge.ConstituencyChild.NodeRef -> {
                            rel.setAttribute("target", child.targetId)
                        }
                        is ConstituencyParserBridge.ConstituencyChild.MorphoRef -> {
                            rel.setAttribute("uri", "morpho.xml#${child.morphoId}")
                        }
                    }

                    spanNode.appendChild(rel)
                }

                spanList.appendChild(spanNode)
            }
        }

        val transformerFactory = TransformerFactory.newInstance()
        val transformer = transformerFactory.newTransformer()
        transformer.setOutputProperty(OutputKeys.INDENT, "yes")
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no")
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "3")
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

        // Build offset-to-index mapping for resolving dependency HEADs
        val offsetToIndex = mutableMapOf<String, Int>()
        tokensArr.forEachIndexed { index, span ->
            offsetToIndex["${span.from}-${span.to}"] = index + 1 // CoNLL-U is 1-indexed
        }
        // Take a snapshot of the morpho map to avoid concurrent modification while iterating
        val morphoSnapshot: Map<String, MorphoSpan> = morpho[docId]?.toMap() ?: emptyMap()
        fun resolveHeadValue(raw: String?): String {
            if (raw == null || raw == "_") return "_"
            return if (raw.contains("-")) {
                val idx = offsetToIndex[raw]
                if (idx != null) idx.toString() else "0"
            } else raw
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
             // Token text safely
             var tokenText: String = if (textVal != null) {
                 val safeFrom = span.from.coerceIn(0, textVal.length)
                 val safeTo = span.to.coerceIn(safeFrom, textVal.length)
                 textVal.substring(safeFrom, safeTo)
             } else "_"

             if (tokenText.isBlank()) {
                 LOGGER.fine("Replacing empty/blank token at offset ${span.from}-${span.to} in document $docId with underscore")
                 tokenText = "_"
             }

            if (morphoSnapshot.containsKey("${span.from}-${span.to}")) {
                val mfs = morphoSnapshot["${span.from}-${span.to}"]
                if (mfs != null) {
                    val miscWithOffset = if (annotationWorkerPool != null && outputFormat == OutputFormat.KORAPXML) {
                        val existing = mfs.misc ?: "_"
                        if (existing == "_") "Offset=${span.from}-${span.to}" else "${existing}|Offset=${span.from}-${span.to}"
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
                                resolveHeadValue(mfs.head),
                                mfs.deprel ?: "_",
                                mfs.deps ?: "_",
                                miscWithOffset,
                                columns
                            )
                        )
                    } catch (e: NullPointerException) {
                        LOGGER.warning("NPE processing morpho for $docId at ${span.from}-${span.to}: ${e.message}")
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
        val output = StringBuilder()
        if (extractMetadataRegex.isNotEmpty()) {
            output.append(metadata[docId]?.joinToString("\t", postfix = "\t") ?: "")
        }
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
                if (output.isNotEmpty()) output.setCharAt(output.length - 1, '\n') else output.append("\n")
                if (extractMetadataRegex.isNotEmpty() && real_token_index < tokens[docId]!!.size - 1) {
                    output.append(metadata[docId]?.joinToString("\t", postfix = "\t") ?: "")
                }
                sentence_index++
            }
            val safeFrom = span.from.coerceIn(0, texts[docId]!!.length)
            val safeTo = span.to.coerceIn(safeFrom, texts[docId]!!.length)
            if (useLemma && morpho[docId] != null) {
                val key = "${span.from}-${span.to}"
                val lemmaVal = morpho[docId]!![key]?.lemma
                if (lemmaVal != null && lemmaVal != "_") {
                    output.append(lemmaVal).append(' ')
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
        if (output.isNotEmpty()) output.deleteCharAt(output.length - 1)
        return output
    }

    private fun nowOutput(docId: String): StringBuilder {
        var token_index = 0
        var real_token_index = 0
        var sentence_index = 0
        val output = StringBuilder()

        output.append("@@$docId ")

        if (texts[docId] == null) {
            tokens[docId]?.forEach { span ->
                if (sentences[docId] != null && (sentence_index >= sentences[docId]!!.size || span.from >= sentences[docId]!![sentence_index].to)) {
                    if (output.isNotEmpty() && !output.endsWith("@@$docId ")) output.append(" <p> ")
                    sentence_index++
                }
                val key = "${span.from}-${span.to}"
                val lemmaVal = morpho[docId]?.get(key)?.lemma
                output.append((lemmaVal?.takeIf { it != "_" } ?: "_"), " ")
            }
            if (output.isNotEmpty() && output.endsWith(" ")) output.deleteCharAt(output.length - 1)
            return output
        }

        tokens[docId]?.forEach { span ->
            token_index++
            if (sentences[docId] != null && (sentence_index >= sentences[docId]!!.size || span.from >= sentences[docId]!![sentence_index].to)) {
                if (output.isNotEmpty() && !output.endsWith("@@$docId ")) output.append(" <p> ")
                sentence_index++
            }
            val safeFrom = span.from.coerceIn(0, texts[docId]!!.length)
            val safeTo = span.to.coerceIn(safeFrom, texts[docId]!!.length)
            if (useLemma && morpho[docId] != null) {
                val key = "${span.from}-${span.to}"
                val lemmaVal = morpho[docId]!![key]?.lemma
                if (lemmaVal != null && lemmaVal != "_") {
                    output.append(lemmaVal).append(' ')
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
        if (output.isNotEmpty() && output.endsWith(" ")) output.deleteCharAt(output.length - 1)
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

    private fun extractSpans(spans: NodeList, docId: String): Array<Span> {
        val list = ArrayList<Span>()
        IntStream.range(0, spans.length).forEach { idx ->
            val node = spans.item(idx)
            if (node is Element) {
                val fromAttr = node.getAttribute("from")
                val toAttr = node.getAttribute("to")
                if (fromAttr.isNullOrEmpty() || toAttr.isNullOrEmpty()) {
                    LOGGER.warning("[$docId] Skipping span with empty from/to attribute: from='$fromAttr' to='$toAttr'")
                } else {
                    try {
                        val from = Integer.parseInt(fromAttr)
                        val to = Integer.parseInt(toAttr)
                        list.add(Span(from, to))
                    } catch (e: NumberFormatException) {
                        LOGGER.warning("[$docId] Skipping span with invalid numeric offsets: from='$fromAttr' to='$toAttr' : ${e.message}")
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
                            "certainty" -> if(fs.misc == "_") fs.misc = value
                        }
                    }
                res[fromTo] = fs
            }
        return res
    }

    private fun extractDependencySpans(
        depSpans: NodeList
    ): MutableMap<String, MorphoSpan> {
        val res: MutableMap<String, MorphoSpan> = HashMap()
        IntStream.range(0, depSpans.length).mapToObj(depSpans::item)
            .filter { node -> node is Element }
            .forEach { node ->
                node as Element
                val fromTo = "${node.getAttribute("from")}-${node.getAttribute("to")}"

                val relElements = node.getElementsByTagName("rel")
                if (relElements.length > 0) {
                    val rel = relElements.item(0) as Element
                    val deprel = rel.getAttribute("label")

                    val innerSpans = rel.getElementsByTagName("span")
                    var head: String? = null
                    if (innerSpans.length > 0) {
                        val innerSpan = innerSpans.item(0) as Element
                        val headFrom = innerSpan.getAttribute("from")
                        val headTo = innerSpan.getAttribute("to")
                        head = "$headFrom-$headTo"
                    }

                    if (head != null || deprel != null) {
                        res[fromTo] = MorphoSpan(
                            head = head ?: "_",
                            deprel = deprel ?: "_"
                        )
                    }
                }
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
                    }

                }
                if (res.isNotEmpty()) {
                    if (miscLocal.containsKey(from)) {
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

        val docId = task?.docId
        val entryPathAndFoundry = task?.entryPath?.split("|") ?: listOf(null, null)
        val entryPath = entryPathAndFoundry.getOrNull(0)
        val foundry = entryPathAndFoundry.getOrNull(1) ?: "base"

        if (docId == null || entryPath == null) {
            LOGGER.fine("Missing metadata from task! docId=$docId, entryPath=$entryPath, task=$task")
            return
        }

        val morphoSpans = mutableMapOf<String, MorphoSpan>()
        val lines = annotatedConllu.lines()
        var currentStartOffsets: List<Int>? = null
        var currentEndOffsets: List<Int>? = null
        var tokenIndexInSentence = 0
        val sentenceSpans = mutableListOf<Span>()
        var sentenceStartOffset: Int? = null
        var sentenceEndOffset: Int? = null

        for (line in lines) {
            when {
                line.startsWith("# start_offsets =") -> {
                    val offsetsStr = line.substring("# start_offsets =".length).trim()
                    val allOffsets = offsetsStr.split(Regex("\\s+")).mapNotNull { it.toIntOrNull() }
                    sentenceStartOffset = allOffsets.firstOrNull()
                    currentStartOffsets = if (allOffsets.size > 1) allOffsets.drop(1) else allOffsets
                    tokenIndexInSentence = 0
                }
                line.startsWith("# end_offsets =") -> {
                    val offsetsStr = line.substring("# end_offsets =".length).trim()
                    val allOffsets = offsetsStr.split(Regex("\\s+")).mapNotNull { it.toIntOrNull() }
                    sentenceEndOffset = allOffsets.firstOrNull()
                    currentEndOffsets = if (allOffsets.size > 1) allOffsets.drop(1) else emptyList()
                }
                line.isEmpty() -> {
                    // Sentence boundary: record the sentence span if available
                    if (sentenceStartOffset != null && sentenceEndOffset != null) {
                        sentenceSpans.add(Span(sentenceStartOffset!!, sentenceEndOffset!!))
                    }
                    sentenceStartOffset = null
                    sentenceEndOffset = null
                    currentStartOffsets = null
                    currentEndOffsets = null
                    tokenIndexInSentence = 0
                }
                !line.startsWith("#") -> {
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

                    if (currentStartOffsets != null && currentEndOffsets != null &&
                        tokenIndexInSentence < currentStartOffsets.size &&
                        tokenIndexInSentence < currentEndOffsets.size) {

                        val spanFrom = currentStartOffsets[tokenIndexInSentence]
                        val spanTo = currentEndOffsets[tokenIndexInSentence]
                        val spanKey = "$spanFrom-$spanTo"

                        morphoSpans[spanKey] = MorphoSpan(lemma, upos, xpos, feats, head, deprel, deps, misc)
                        tokenIndexInSentence++
                    }
                }
            }
        }

        // If last sentence did not end with an empty line, capture it now
        if (sentenceStartOffset != null && sentenceEndOffset != null) {
            sentenceSpans.add(Span(sentenceStartOffset!!, sentenceEndOffset!!))
        }

        if (morphoSpans.isEmpty()) {
            LOGGER.warning("No morpho spans found in annotated output for $docId, skipping")
            return
        }

        if (morphoZipOutputStream == null) {
            LOGGER.severe("morphoZipOutputStream is null! Cannot write to ZIP. This should have been initialized in processZipFile.")
            return
        }

        if (dBuilder == null) {
            dbFactory = DocumentBuilderFactory.newInstance()
            dBuilder = dbFactory!!.newDocumentBuilder()
        }

        val tempDocId = "_temp_annotated_$docId"
        morpho[tempDocId] = morphoSpans

        val hasDependencies = morphoSpans.values.any { span ->
            span.head != null && span.head != "_" && span.deprel != null && span.deprel != "_"
        }

        // Prefer sentence spans from the comments; fallback to whole-document span if none detected
        if (hasDependencies && morphoSpans.isNotEmpty()) {
            if (sentenceSpans.isNotEmpty()) {
                sentences[tempDocId] = sentenceSpans.toTypedArray()
            } else {
                val allOffsets = morphoSpans.keys.map { key ->
                    val parts = key.split("-")
                    Pair(parts[0].toInt(), parts[1].toInt())
                }
                val minOffset = allOffsets.minOfOrNull { it.first } ?: 0
                val maxOffset = allOffsets.maxOfOrNull { it.second } ?: 0
                sentences[tempDocId] = arrayOf(Span(minOffset, maxOffset))
            }
        }

        try {
            val morphoXmlOutput = korapXmlMorphoOutput(foundry, tempDocId)
            val fixedMorphoXml = morphoXmlOutput.toString().replace(
                "docid=\"$tempDocId\"",
                "docid=\"$docId\""
            )

            val morphoEntryPath = docId.replace(Regex("[_.]"), "/") + "/$foundry/morpho.xml"

            val morphoZipEntry = ZipArchiveEntry(morphoEntryPath)
            morphoZipEntry.unixMode = ZIP_ENTRY_UNIX_MODE
            synchronized(morphoZipOutputStream!!) {
                morphoZipOutputStream!!.putArchiveEntry(morphoZipEntry)
                morphoZipOutputStream!!.write(fixedMorphoXml.toByteArray())
                morphoZipOutputStream!!.closeArchiveEntry()
            }
            val written = docsWrittenToZip.incrementAndGet()
            if (!quiet) progressBar?.step()
        } catch (e: Exception) {
            LOGGER.severe("ERROR generating/writing morpho.xml: ${e.message}")
            e.printStackTrace()
        }

        if (morpho[tempDocId]?.values?.any { it.head != null && it.head != "_" && it.deprel != null && it.deprel != "_" } == true) {
            try {
                val dependencyXmlOutput = korapXmlDependencyOutput(foundry, tempDocId)
                val fixedDependencyXml = dependencyXmlOutput.toString().replace(
                    "docid=\"$tempDocId\"",
                    "docid=\"$docId\""
                )

                val dependencyEntryPath = docId.replace(Regex("[_.]"), "/") + "/$foundry/dependency.xml"

                val dependencyZipEntry = ZipArchiveEntry(dependencyEntryPath)
                dependencyZipEntry.unixMode = ZIP_ENTRY_UNIX_MODE
                synchronized(morphoZipOutputStream!!) {
                    morphoZipOutputStream!!.putArchiveEntry(dependencyZipEntry)
                    morphoZipOutputStream!!.write(fixedDependencyXml.toByteArray())
                    morphoZipOutputStream!!.closeArchiveEntry()
                }
            } catch (e: Exception) {
                LOGGER.severe("ERROR generating/writing dependency.xml: ${e.message}")
                e.printStackTrace()
            }
        }

        morpho.remove(tempDocId)
        sentences.remove(tempDocId)
    }

    // Collect structural spans from structure.xml for krill format
    private fun collectKrillStructureSpans(docId: String, spans: NodeList) {
        // Skip if already output (thread-safe check with ConcurrentHashMap.KeySet)
        if (outputTexts.contains(docId)) return

        val textData = krillData.getOrPut(docId) {
            KrillTextData(textId = docId)
        }

        synchronized(textData) {
            // Only collect if not already collected (avoid duplicates from multiple ZIP processing)
            if (textData.structureSpans.isNotEmpty()) {
                LOGGER.fine("Structure spans already collected for $docId, skipping")
                return
            }
            for (i in 0 until spans.length) {
                val span = spans.item(i) as? Element ?: continue

                val from = span.getAttribute("from").toIntOrNull() ?: continue
                val to = span.getAttribute("to").toIntOrNull() ?: continue
                val level = span.getAttribute("l").toIntOrNull() ?: 0

                // Extract span name from fs/f[@name='name']
                val fsElements = span.getElementsByTagName("fs")
                if (fsElements.length == 0) continue

                val fs = fsElements.item(0) as Element
                val fElements = fs.getElementsByTagName("f")

                var spanName: String? = null
                val attributes = mutableMapOf<String, String>()

                for (j in 0 until fElements.length) {
                    val f = fElements.item(j) as Element
                    val fName = f.getAttribute("name")

                    when (fName) {
                        "name" -> spanName = f.textContent
                        "attr" -> {
                            // Extract attributes from nested fs
                            val attrFs = f.getElementsByTagName("fs")
                            if (attrFs.length > 0) {
                                val attrFsElement = attrFs.item(0) as Element
                                val attrFElements = attrFsElement.getElementsByTagName("f")
                                for (k in 0 until attrFElements.length) {
                                    val attrF = attrFElements.item(k) as Element
                                    val attrName = attrF.getAttribute("name")
                                    val attrValue = attrF.textContent
                                    attributes[attrName] = attrValue
                                }
                            }
                        }
                    }
                }

                if (spanName != null) {
                    textData.structureSpans.add(StructureSpan(
                        layer = "dereko/s:$spanName",
                        from = from,
                        to = to,
                        tokenFrom = -1,  // Will be resolved later
                        tokenTo = -1,    // Will be resolved later
                        depth = level,
                        attributes = attributes
                    ))
                }
            }

            LOGGER.fine("Collected ${textData.structureSpans.size} structural spans for $docId")
        }
    }

    private data class ConstituencyNode(
        val id: String,
        val from: Int,
        val to: Int,
        val label: String,
        val children: MutableList<String> = mutableListOf()
    )

    private fun collectSentences(docId: String, foundry: String, spans: NodeList) {
        if (outputTexts.contains(docId)) return

        val textData = krillData.getOrPut(docId) {
            KrillTextData(textId = docId)
        }

        synchronized(textData) {
            if (textData.sentencesCollectedByFoundry.contains(foundry)) return
            for (i in 0 until spans.length) {
                val span = spans.item(i) as? Element ?: continue
                val from = span.getAttribute("from").toIntOrNull() ?: continue
                val to = span.getAttribute("to").toIntOrNull() ?: continue
                textData.structureSpans.add(
                    StructureSpan(
                        layer = "$foundry/s:s",
                        from = from,
                        to = to,
                        tokenFrom = -1,
                        tokenTo = -1,
                        depth = 0,
                        attributes = emptyMap()
                    )
                )
            }
            textData.sentencesCollectedByFoundry.add(foundry)
        }
    }

    private fun collectConstituency(docId: String, foundry: String, spans: NodeList) {
        if (outputTexts.contains(docId)) return

        val nodesById = mutableMapOf<String, ConstituencyNode>()
        val nonRootIds = mutableSetOf<String>()

        for (i in 0 until spans.length) {
            val span = spans.item(i) as? Element ?: continue
            val id = span.getAttribute("id")
            if (id.isNullOrBlank()) continue
            val from = span.getAttribute("from").toIntOrNull() ?: continue
            val to = span.getAttribute("to").toIntOrNull() ?: continue

            val fsList = span.getElementsByTagName("fs")
            if (fsList.length == 0) continue
            val fsElement = fsList.item(0) as? Element ?: continue
            val fElements = fsElement.getElementsByTagName("f")
            var label: String? = null
            for (j in 0 until fElements.length) {
                val f = fElements.item(j) as? Element ?: continue
                if (f.getAttribute("name") == "const") {
                    label = f.textContent?.trim()
                    break
                }
            }
            if (label.isNullOrBlank()) continue

            val node = ConstituencyNode(id, from, to, label)

            val relElements = span.getElementsByTagName("rel")
            for (j in 0 until relElements.length) {
                val rel = relElements.item(j) as? Element ?: continue
                if (rel.getAttribute("label") != "dominates") continue
                val target = rel.getAttribute("target")
                if (!target.isNullOrBlank()) {
                    node.children.add(target)
                    nonRootIds.add(target)
                } else {
                    val uri = rel.getAttribute("uri")
                    if (!uri.isNullOrBlank()) {
                        val normalized = uri.removePrefix("morpho.xml#")
                        if (normalized.isNotBlank()) {
                            nonRootIds.add(normalized)
                        }
                    }
                }
            }
            nodesById[id] = node
        }

        if (nodesById.isEmpty()) return

        val textData = krillData.getOrPut(docId) {
            KrillTextData(textId = docId)
        }

        synchronized(textData) {
            if (textData.constituencyCollectedByFoundry.contains(foundry)) return
            LOGGER.fine("Collecting constituency for $docId from foundry $foundry: ${nodesById.size} nodes, roots=${nodesById.keys.count { it !in nonRootIds }}")

            fun traverse(nodeId: String, depth: Int) {
                val node = nodesById[nodeId] ?: return
                textData.structureSpans.add(
                    StructureSpan(
                        layer = "$foundry/c:${node.label}",
                        from = node.from,
                        to = node.to,
                        tokenFrom = -1,
                        tokenTo = -1,
                        depth = depth,
                        attributes = emptyMap()
                    )
                )
                node.children.forEach { childId ->
                    traverse(childId, depth + 1)
                }
            }

            val rootIds = nodesById.keys.filter { it !in nonRootIds }
            rootIds.forEach { traverse(it, 0) }
            textData.constituencyCollectedByFoundry.add(foundry)
        }
    }

    // Collect rich metadata from header.xml for krill format
    private fun collectKrillMetadata(docId: String, headerRoot: Element) {
        if (outputTexts.contains(docId)) return

        val textData = krillData.getOrPut(docId) { KrillTextData(textId = docId) }

        synchronized(textData) {
            val metadata = textData.headerMetadata
            val analytic = headerRoot.firstElement("analytic")
            val monogr = headerRoot.firstElement("monogr")
            val textDesc = headerRoot.firstElement("textDesc")
            val textClassElement = headerRoot.firstElement("textClass")

            metadata.putIfNotBlank("author", analytic.firstText("h.author") ?: monogr.firstText("h.author"))

            val mainTitle = analytic.firstText("h.title") { it.getAttribute("type") == "main" }
                ?: analytic.firstText("h.title")
                ?: monogr.firstText("h.title") { it.getAttribute("type") == "main" }
                ?: monogr.firstText("h.title")
            metadata.putIfNotBlank("title", mainTitle)

            metadata.putIfNotBlank(
                "subTitle",
                analytic.firstText("h.title") { it.getAttribute("type") == "sub" }
                    ?: monogr.firstText("h.title") { it.getAttribute("type") == "sub" }
            )

            val translator = headerRoot.firstText("editor") { it.getAttribute("role") == "translator" }
            if (translator != null) {
                metadata["translator"] = translator
            } else {
                metadata.putIfNotBlank("editor", analytic.firstText("editor") ?: monogr.firstText("editor"))
            }

            metadata.putIfNotBlank("publisher", headerRoot.firstText("publisher"))
            metadata.putIfNotBlank("distributor", headerRoot.firstText("distributor"))
            metadata.putIfNotBlank("availability", headerRoot.firstText("availability"))
            metadata.putIfNotBlank("ISBN", headerRoot.firstText("idno") { it.getAttribute("type") == "ISBN" })

            headerRoot.firstElement("reference") { it.getAttribute("type") == "complete" }
                ?.textContent?.trim()?.takeIf { it.isNotEmpty() }?.let { metadata["reference"] = it }

            headerRoot.firstElement("idno") { it.getAttribute("type") == "URN" }
                ?.textContent?.trim()?.takeIf { it.isNotEmpty() }?.let { urn ->
                    metadata["URN"] = mapOf("urn" to urn, "url" to "http://nbn-resolving.de/$urn")
                }

            metadata.putIfNotBlank("textType", textDesc.firstText("textType"))
            metadata.putIfNotBlank("textDomain", textDesc.firstText("textDomain"))
            metadata.putIfNotBlank("textTypeArt", textDesc.firstText("textTypeArt"))
            metadata.putIfNotBlank("textTypeRef", textDesc.firstText("textTypeRef"))
            metadata.putIfNotBlank("textColumn", textDesc.firstText("column"))

            headerRoot.firstElement("pubPlace")?.let { placeElement ->
                placeElement.textContent?.trim()?.takeIf { it.isNotEmpty() }?.let { metadata["pubPlace"] = it }
                placeElement.getAttribute("key").takeIf { it.isNotBlank() }?.let { metadata["pubPlaceKey"] = it }
            }

            val awards = headerRoot.childElements("note")
                .mapNotNull { note ->
                    val subtype = note.getAttribute("subtype")
                    if (note.getAttribute("type") == "award" && subtype.isNotBlank()) subtype.trim() else null
                }.toList()
            if (awards.isNotEmpty()) {
                metadata["award"] = awards
            }

            val textClassTopics = textClassElement.collectCatRefTopics()
            val fallbackTopics = if (textClassTopics.isEmpty()) headerRoot.collectCatRefTopics() else emptyList()
            val finalTopics = if (textClassTopics.isNotEmpty()) textClassTopics else fallbackTopics
            if (finalTopics.isNotEmpty()) {
                metadata["textClass"] = finalTopics
            }

            headerRoot.firstText("creatDate")?.replace(".", "-")?.let {
                metadata["creationDate"] = it
            }

            var year: String? = null
            var month: String? = null
            var day: String? = null
            headerRoot.childElements("pubDate").forEach { element ->
                val value = element.textContent?.trim()?.takeIf { it.isNotEmpty() } ?: return@forEach
                when (element.getAttribute("type")) {
                    "year" -> year = value
                    "month" -> month = value
                    "day" -> day = value
                }
            }
            if (year != null && month != null && day != null) {
                metadata["pubDate"] = "${year}-${month!!.padStart(2, '0')}-${day!!.padStart(2, '0')}"
            }

            headerRoot.firstElement("ref") { it.getAttribute("type") == "page_url" }
                ?.getAttribute("target")?.takeIf { it.isNotBlank() }?.let { metadata["externalLink"] = it }

            if (!metadata.containsKey("language")) {
                metadata["language"] = "de"
            }

            if (!metadata.containsKey("textType")) {
                val textTypeArt = metadata["textTypeArt"] as? String
                if (textTypeArt != null) {
                    metadata["textType"] = textTypeArt + "en"
                }
            }

            LOGGER.fine("Collected ${metadata.size} metadata fields for $docId")
        }
    }

    // Collect corpus-level metadata from corpus header
    private fun collectCorpusMetadata(corpusSigle: String, headerRoot: Element) {
        val metadata = corpusMetadata.getOrPut(corpusSigle) { mutableMapOf() }

        synchronized(metadata) {
            metadata.putIfNotBlank("corpusTitle", headerRoot.firstText("c.title"))
            metadata.putIfNotBlank(
                "corpusSubTitle",
                headerRoot.firstText("c.title") { it.getAttribute("type") == "sub" }
            )
            metadata.putIfNotBlank("corpusAuthor", headerRoot.firstText("h.author"))
            metadata.putIfNotBlank("corpusEditor", headerRoot.firstElement("monogr").firstText("editor"))
            metadata.putIfNotBlank("publisher", headerRoot.firstText("publisher"))
            metadata.putIfNotBlank("distributor", headerRoot.firstText("distributor"))
            metadata.putIfNotBlank("textType", headerRoot.firstElement("textDesc").firstText("textType"))
            LOGGER.fine("Collected ${metadata.size} corpus-level metadata fields for $corpusSigle")
        }
    }

    // Collect document-level metadata from doc header
    private fun collectDocMetadata(docSigle: String, headerRoot: Element) {
        val metadata = docMetadata.getOrPut(docSigle) { mutableMapOf() }

        synchronized(metadata) {
            metadata.putIfNotBlank("docTitle", headerRoot.firstText("d.title"))
            metadata.putIfNotBlank("docAuthor", headerRoot.firstText("h.author"))
            LOGGER.fine("Collected ${metadata.size} doc-level metadata fields for $docSigle")
        }
    }

    // Collect base text data (text, tokens, sentences) for krill format
    private fun collectKrillBaseData(docId: String) {
        // Skip if already output (thread-safe check with ConcurrentHashMap.KeySet)
        if (outputTexts.contains(docId)) return

        LOGGER.info("Processing base data for $docId: text=${texts[docId] != null}, tokens=${tokens[docId] != null}, sentences=${sentences[docId] != null}, foundry=base")

        val textData = krillData.getOrPut(docId) {
            KrillTextData(textId = docId)
        }

        synchronized(textData) {
            // Capture values locally to avoid TOCTOU race conditions
            val text = texts[docId]
            if (text != null) {
                textData.textContent = text.toString()
            }
            val tokenArray = tokens[docId]
            if (tokenArray != null) {
                textData.tokens = tokenArray
            }
            val sentenceArray = sentences[docId]
            if (sentenceArray != null) {
                textData.sentences = sentenceArray
            }
            // Collect metadata
            val metadataArray = metadata[docId]
            if (metadataArray != null) {
                metadataArray.forEachIndexed { index, value ->
                    textData.headerMetadata["field_$index"] = value
                }
            }
            LOGGER.info("  Collected base text data for $docId: ${textData.textContent?.length ?: 0} chars, ${textData.tokens?.size ?: 0} tokens")
        }

        // Release base data from memory (but keep morpho for later foundries)
        texts.remove(docId)
        tokens.remove(docId)
        sentences.remove(docId)
        fnames.remove(docId)
        metadata.remove(docId)
        extraFeatures.remove(docId)
    }

    // Extract the appropriate foundry name for a given annotation layer
    // For combined foundries like "marmot-malt", split into morpho (marmot) and dependency (malt) parts
    private fun getFoundryForLayer(foundry: String, layer: String): String {
        return if ("-" in foundry) {
            val parts = foundry.split("-")
            when (layer) {
                "morpho" -> parts[0]  // First part is morphology tagger (e.g., "marmot")
                "dependency" -> parts.getOrElse(1) { parts[0] }  // Second part is parser (e.g., "malt")
                else -> foundry
            }
        } else {
            foundry  // Single foundry used for all layers
        }
    }

    // Collect morpho data directly from parsed data (for krill format, bypasses shared morpho map)
    // This version takes the morpho data as a parameter to avoid contamination from other foundries
    private fun collectKrillMorphoDataDirect(docId: String, foundry: String, morphoDataMap: MutableMap<String, MorphoSpan>, annotationType: String = "morpho") {
        // Skip if already output (thread-safe check with ConcurrentHashMap.KeySet)
        if (outputTexts.contains(docId)) return

        LOGGER.info("Collecting krill $annotationType data (direct) for $docId, foundry=$foundry, morpho=${morphoDataMap.size}")

        val textData = krillData.getOrPut(docId) {
            KrillTextData(textId = docId)
        }

        if (morphoDataMap.isNotEmpty()) {
            // Copy the data, filtering by annotation type
            val morphoDataCopy = morphoDataMap.mapValues { (_, span) ->
                // Create a filtered copy of the span based on annotation type
                val filteredSpan = MorphoSpan()
                if (annotationType == "morpho") {
                    // Copy only morphological annotations (POS, lemma, features)
                    filteredSpan.lemma = span.lemma
                    filteredSpan.upos = span.upos
                    filteredSpan.xpos = span.xpos
                    filteredSpan.feats = span.feats
                    filteredSpan.misc = span.misc
                } else if (annotationType == "dependency") {
                    // Copy only dependency annotations (head, deprel)
                    filteredSpan.head = span.head
                    filteredSpan.deprel = span.deprel
                }
                filteredSpan
            }.toMutableMap()

            synchronized(textData) {
                // Merge with existing morpho data for this foundry (don't overwrite)
                val existingFoundryData = textData.morphoByFoundry[foundry]
                if (existingFoundryData == null) {
                    // First time collecting this foundry - just copy
                    textData.morphoByFoundry[foundry] = morphoDataCopy
                    LOGGER.info("  Added ${morphoDataCopy.size} $annotationType annotations for $docId from foundry $foundry, total foundries=${textData.morphoByFoundry.keys}")
                } else {
                    // Merge with existing data (e.g., adding dependencies to existing morpho)
                    var mergedCount = 0
                    var newCount = 0
                    morphoDataCopy.forEach { (key, newSpan) ->
                        val existingSpan = existingFoundryData[key]
                        if (existingSpan != null) {
                            // Merge: add new annotations based on type
                            if (annotationType == "dependency") {
                                // Only update dependency fields
                                if (newSpan.head != null && newSpan.head != "_") existingSpan.head = newSpan.head
                                if (newSpan.deprel != null && newSpan.deprel != "_") existingSpan.deprel = newSpan.deprel
                            } else if (annotationType == "morpho") {
                                // Only update morphological fields (check for "_" since MorphoSpan defaults to "_", not null)
                                if (newSpan.lemma != null && newSpan.lemma != "_" && (existingSpan.lemma == null || existingSpan.lemma == "_")) existingSpan.lemma = newSpan.lemma
                                if (newSpan.upos != null && newSpan.upos != "_" && (existingSpan.upos == null || existingSpan.upos == "_")) existingSpan.upos = newSpan.upos
                                if (newSpan.xpos != null && newSpan.xpos != "_" && (existingSpan.xpos == null || existingSpan.xpos == "_")) existingSpan.xpos = newSpan.xpos
                                if (newSpan.feats != null && newSpan.feats != "_" && (existingSpan.feats == null || existingSpan.feats == "_")) existingSpan.feats = newSpan.feats
                                if (newSpan.misc != null && newSpan.misc != "_" && (existingSpan.misc == null || existingSpan.misc == "_")) existingSpan.misc = newSpan.misc
                            }
                            mergedCount++
                        } else {
                            // New span not in existing data
                            existingFoundryData[key] = newSpan
                            newCount++
                        }
                    }
                    LOGGER.info("  Merged ${morphoDataCopy.size} $annotationType annotations for $docId from foundry $foundry ($mergedCount merged, $newCount new), total foundries=${textData.morphoByFoundry.keys}")
                }
            }
        }
    }

    // Collect morpho data from a specific foundry for krill format (OLD VERSION - reads from shared morpho map)
    // annotationType: "morpho" = collect POS/lemma/features, "dependency" = collect head/deprel only
    private fun collectKrillMorphoData(docId: String, foundry: String, annotationType: String = "morpho") {
        // Skip if already output (thread-safe check with ConcurrentHashMap.KeySet)
        if (outputTexts.contains(docId)) return

        LOGGER.info("Collecting krill $annotationType data for $docId, foundry=$foundry, morpho=${morpho[docId]?.size ?: 0}")

        val textData = krillData.getOrPut(docId) {
            KrillTextData(textId = docId)
        }

        val morphoDataMap = morpho[docId]
        if (morphoDataMap != null && morphoDataMap.isNotEmpty()) {
            // Synchronize on morpho map to avoid concurrent modification
            synchronized(morphoDataMap) {
                // Copy the data while holding the lock, filtering by annotation type
                val morphoDataCopy = morphoDataMap.mapValues { (_, span) ->
                    // Create a filtered copy of the span based on annotation type
                    val filteredSpan = MorphoSpan()
                    if (annotationType == "morpho") {
                        // Copy only morphological annotations (POS, lemma, features)
                        filteredSpan.lemma = span.lemma
                        filteredSpan.upos = span.upos
                        filteredSpan.xpos = span.xpos
                        filteredSpan.feats = span.feats
                        filteredSpan.misc = span.misc
                    } else if (annotationType == "dependency") {
                        // Copy only dependency annotations (head, deprel)
                        filteredSpan.head = span.head
                        filteredSpan.deprel = span.deprel
                    }
                    filteredSpan
                }.toMutableMap()

                synchronized(textData) {
                    // Merge with existing morpho data for this foundry (don't overwrite)
                    val existingFoundryData = textData.morphoByFoundry[foundry]
                    if (existingFoundryData == null) {
                        // First time collecting this foundry - just copy
                        textData.morphoByFoundry[foundry] = morphoDataCopy
                        LOGGER.info("  Added ${morphoDataCopy.size} $annotationType annotations for $docId from foundry $foundry, total foundries=${textData.morphoByFoundry.keys}")
                    } else {
                        // Merge with existing data (e.g., adding dependencies to existing morpho)
                        var mergedCount = 0
                        var newCount = 0
                        morphoDataCopy.forEach { (key, newSpan) ->
                            val existingSpan = existingFoundryData[key]
                            if (existingSpan != null) {
                                // Merge: add new annotations based on type
                                if (annotationType == "dependency") {
                                    // Only update dependency fields
                                    if (newSpan.head != null && newSpan.head != "_") existingSpan.head = newSpan.head
                                    if (newSpan.deprel != null && newSpan.deprel != "_") existingSpan.deprel = newSpan.deprel
                                } else if (annotationType == "morpho") {
                                    // Only update morphological fields (check for "_" since MorphoSpan defaults to "_", not null)
                                    if (newSpan.lemma != null && newSpan.lemma != "_" && (existingSpan.lemma == null || existingSpan.lemma == "_")) existingSpan.lemma = newSpan.lemma
                                    if (newSpan.upos != null && newSpan.upos != "_" && (existingSpan.upos == null || existingSpan.upos == "_")) existingSpan.upos = newSpan.upos
                                    if (newSpan.xpos != null && newSpan.xpos != "_" && (existingSpan.xpos == null || existingSpan.xpos == "_")) existingSpan.xpos = newSpan.xpos
                                    if (newSpan.feats != null && newSpan.feats != "_" && (existingSpan.feats == null || existingSpan.feats == "_")) existingSpan.feats = newSpan.feats
                                    if (newSpan.misc != null && newSpan.misc != "_" && (existingSpan.misc == null || existingSpan.misc == "_")) existingSpan.misc = newSpan.misc
                                }
                                mergedCount++
                            } else {
                                // New span not in existing data
                                existingFoundryData[key] = newSpan
                                newCount++
                            }
                        }
                        LOGGER.info("  Merged ${morphoDataCopy.size} $annotationType annotations for $docId from foundry $foundry ($mergedCount merged, $newCount new), total foundries=${textData.morphoByFoundry.keys}")
                    }
                }
            }
        }

        // Note: Don't clear morpho[docId] here because we might need it for subsequent layers
        // (e.g., when processing marmot-malt, morpho.xml is collected as "marmot" first,
        // then dependency.xml needs the data to collect as "malt")
        // The data will be cleared when the document is fully processed
    }

    // Old collectKrillData - no longer used, kept for reference
    private fun collectKrillData(docId: String, foundry: String) {
        // Skip if already output (thread-safe check with ConcurrentHashMap.KeySet)
        if (outputTexts.contains(docId)) return

        LOGGER.info("Collecting krill data for $docId, foundry=$foundry, morpho=${morpho[docId]?.size ?: 0}")

        // Get or create KrillTextData for this text
        val wasNew = krillData[docId] == null
        val textData = krillData.getOrPut(docId) {
            LOGGER.info("  Creating new KrillTextData for $docId")
            KrillTextData(textId = docId)
        }
        if (!wasNew) {
            LOGGER.info("  Found existing KrillTextData for $docId, foundries=${textData.morphoByFoundry.keys}")
        }

        // Collect text content (only from base foundry)
        if (foundry == "base" && texts[docId] != null) {
            synchronized(textData) {
                textData.textContent = texts[docId]!!.toString()
                textData.tokens = tokens[docId]
                textData.sentences = sentences[docId]
                LOGGER.info("  Collected base text data for $docId: ${textData.textContent?.length ?: 0} chars, ${textData.tokens?.size ?: 0} tokens")

                // Collect metadata
                if (metadata[docId] != null) {
                    metadata[docId]!!.forEachIndexed { index, value ->
                        textData.headerMetadata["field_$index"] = value
                    }
                }
            }
        }

        // Collect morpho annotations from this foundry
        if (morpho[docId] != null && morpho[docId]!!.isNotEmpty()) {
            synchronized(textData) {
                // Make a copy of the morpho data to preserve it
                textData.morphoByFoundry[foundry] = morpho[docId]!!.toMutableMap()
                LOGGER.info("  Added ${morpho[docId]!!.size} morpho spans for $docId from foundry $foundry, total foundries=${textData.morphoByFoundry.keys}")
            }
        }

        // Release memory for this document
        arrayOf(tokens, texts, sentences, morpho, fnames, metadata, extraFeatures).forEach { map ->
            if (map === morpho) {
                morpho[docId]?.clear()
            }
            map.remove(docId)
        }
    }

    // Start timer-based scanner for incremental output
    private fun startIncrementalWriterThread() {
        if (outputFormat != OutputFormat.KRILL || krillTarOutputStream == null) return

        shutdownIncrementalWriter = false
        incrementalOutputScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "KrillWriterThread")
        }

        // Scan for complete texts every 500ms
        incrementalOutputScheduler?.scheduleAtFixedRate({
            try {
                scanAndOutputCompleteTexts()
            } catch (e: Exception) {
                LOGGER.severe("Error in incremental writer: ${e.message}")
                e.printStackTrace()
            }
        }, 500, 500, java.util.concurrent.TimeUnit.MILLISECONDS)

        LOGGER.info("Incremental writer scheduler started (scanning every 500ms)")
    }

    // Scan all texts and output any that are complete
    private fun scanAndOutputCompleteTexts(forceScan: Boolean = false) {
        if ((shutdownIncrementalWriter && !forceScan) || !tarStreamOpen) return

        // Get all texts that we know about (from zipInventory), sorted to match processing order
        // This ensures we check texts in the same order they're being processed
        val allTexts = zipInventory.values.flatten().toSet().sorted()

        var outputCount = 0
        for (textId in allTexts) {
            // Check if shutdown was requested, thread was interrupted, or stream was closed
            if (shutdownIncrementalWriter || Thread.currentThread().isInterrupted || !tarStreamOpen) break

            // Skip if already output
            if (outputTexts.contains(textId)) continue

            // Find all ZIPs that should contain this text
            val relevantZips = zipInventory.filter { (_, texts) -> texts.contains(textId) }.keys

            // Check if all relevant ZIPs have processed this text
            val allProcessed = relevantZips.all { path ->
                processedTextsPerZip[path]?.contains(textId) == true
            }

            if (allProcessed && krillData.containsKey(textId)) {
                // Atomically claim this text for output (add returns false if already present)
                if (outputTexts.add(textId)) {
                    // We successfully claimed it - output now
                    val textData = krillData.remove(textId)
                    if (textData != null) {
                        // Acquire lock to prevent concurrent access with stream closing
                        tarStreamLock.lock()
                        try {
                            // Double-check stream is still open while holding the lock
                            if (tarStreamOpen) {
                                try {
                                    outputKrillText(textId, textData)
                                    incrementalProgressBar?.step()
                                    outputCount++
                                    LOGGER.fine("Output text $textId (processed by ${relevantZips.size} ZIPs, ${krillData.size} still pending)")
                                } catch (e: IOException) {
                                    // Stream may have been closed - stop trying to output
                                    LOGGER.warning("Cannot output text $textId: stream closed")
                                    tarStreamOpen = false
                                    break
                                }
                            }
                        } finally {
                            tarStreamLock.unlock()
                        }
                    }

                    // Clean up tracking data for this text
                    relevantZips.forEach { path ->
                        zipInventory[path]?.remove(textId)
                        processedTextsPerZip[path]?.remove(textId)
                    }
                }
            }
        }

        if (outputCount > 0) {
            LOGGER.fine("Batch output: $outputCount texts (${krillData.size} still pending)")
        }
    }

    // Stop the incremental writer thread
    private fun stopIncrementalWriterThread() {
        if (incrementalOutputScheduler != null) {
            LOGGER.info("Stopping incremental writer scheduler")

            // Set shutdown flag first to prevent scanner from starting new work
            shutdownIncrementalWriter = true

            // Use shutdownNow() to cancel any pending scheduled tasks immediately
            val cancelledTasks = incrementalOutputScheduler?.shutdownNow()
            LOGGER.fine("Cancelled ${cancelledTasks?.size ?: 0} pending scheduled tasks")

            // Wait for currently executing tasks to finish
            // Note: This may take some time if a scan is in progress on a large corpus
            try {
                var waitTime = 0L
                while (!incrementalOutputScheduler!!.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS)) {
                    waitTime++
                    LOGGER.fine("Waiting for writer scheduler to terminate... (${waitTime}s)")
                }
                if (waitTime > 0) {
                    LOGGER.info("Writer scheduler terminated after ${waitTime}s")
                }
            } catch (e: InterruptedException) {
                LOGGER.warning("Interrupted while stopping writer scheduler")
                Thread.currentThread().interrupt()
            }

            // Do one final scan after scheduler has stopped completely
            val remainingBeforeScan = krillData.size
            LOGGER.info("Doing final scan for remaining texts...")
            scanAndOutputCompleteTexts(forceScan = true)
            LOGGER.info("Final scan completed, output ${remainingBeforeScan - krillData.size} texts")

            incrementalOutputScheduler = null
        }
    }

    // Build per-ZIP inventory of which texts each ZIP contains
    // This allows us to know when a text has been processed by all ZIPs that should contain it
    private fun buildZipInventory(zipPaths: Array<String>) {
        LOGGER.info("Building per-ZIP inventory to track text completeness...")
        zipInventory.clear()

        // Show progress bar for ZIP scanning phase
        val scanProgressBar = if (!quiet && zipPaths.size > 1) {
            ProgressBarBuilder()
                .setTaskName("Scanning ZIPs")
                .setInitialMax(zipPaths.size.toLong())
                .setStyle(ProgressBarStyle.COLORFUL_UNICODE_BAR)
                .setUpdateIntervalMillis(500)
                .build()
        } else null

        // Scan ZIPs in parallel for faster startup
        val scanParallelism = (zipParallelism ?: maxThreads).coerceAtLeast(1)
        val executor = java.util.concurrent.Executors.newFixedThreadPool(scanParallelism)

        try {
            val futures = zipPaths.map { zipPath ->
                executor.submit<Pair<String, MutableSet<String>>> {
                    val textsInThisZip = mutableSetOf<String>()
                    LOGGER.info("Scanning $zipPath...")

                    // Determine if this is a base ZIP or annotation foundry
                    val zipName = File(zipPath).name
                    val isAnnotationFoundry = zipName.matches(Regex(".*\\.[^/.]+\\.zip$"))

                    try {
                        val dbFactory = DocumentBuilderFactory.newInstance()
                        val dBuilder = dbFactory.newDocumentBuilder()

                        ApacheZipFile(File(zipPath)).use { zipFile ->
                            val entries = zipFile.entries
                            while (entries.hasMoreElements()) {
                                val entry = entries.nextElement()
                                // For base ZIPs: look for data.xml or tokens.xml
                                // For annotation foundries: also look for morpho.xml or dependency.xml
                                val pattern = if (isAnnotationFoundry) {
                                    Regex(".*(data|tokens|morpho|dependency)\\.xml$")
                                } else {
                                    Regex(".*(data|tokens)\\.xml$")
                                }

                                if (entry.name.matches(pattern)) {
                                    try {
                                        // Parse XML to extract docId attribute
                                        val doc = zipFile.getInputStream(entry).use { inputStream ->
                                            XMLCommentFilterReader(inputStream, "UTF-8").use { reader ->
                                                dBuilder.parse(InputSource(reader))
                                            }
                                        }
                                        doc.documentElement.normalize()
                                        val docId = doc.documentElement.getAttribute("docid")
                                        if (docId.isNotEmpty()) {
                                            textsInThisZip.add(docId)
                                        }
                                    } catch (e: Exception) {
                                        // Skip entries that can't be parsed
                                        LOGGER.fine("Skipped entry ${entry.name}: ${e.message}")
                                    }
                                }
                            }
                        }

                        // Use different wording for base vs annotation foundries
                        if (isAnnotationFoundry) {
                            LOGGER.info("  $zipPath has annotations on ${textsInThisZip.size} texts")
                        } else {
                            LOGGER.info("  $zipPath contains ${textsInThisZip.size} texts")
                        }
                    } catch (e: Exception) {
                        LOGGER.warning("Failed to scan $zipPath: ${e.message}")
                    }

                    scanProgressBar?.step()
                    Pair(zipPath, textsInThisZip)
                }
            }

            // Collect results
            futures.forEach { future ->
                val (zipPath, texts) = future.get()
                zipInventory[zipPath] = texts
            }
        } finally {
            executor.shutdown()
            executor.awaitTermination(1, java.util.concurrent.TimeUnit.HOURS)
        }

        scanProgressBar?.close()

        LOGGER.info("ZIP inventory built: ${zipPaths.size} ZIPs scanned")
        // Calculate total unique texts
        val allTexts = zipInventory.values.flatten().toSet()
        LOGGER.info("  Total unique texts across all ZIPs: ${allTexts.size}")
    }

    // Output a single text to Krill TAR (thread-safe)
    private fun outputKrillText(textId: String, textData: KrillTextData) {
        try {
            // Merge corpus and doc metadata
            val textIdWithSlashes = textData.textId.replace("_", "/").replace(".", "/")
            val corpusSigle = textIdWithSlashes.split("/")[0]
            val docSigle = textIdWithSlashes.split("/").take(2).joinToString("/")

            // Apply corpus-level metadata
            corpusMetadata[corpusSigle]?.forEach { (key, value) ->
                if (!textData.headerMetadata.containsKey(key)) {
                    textData.headerMetadata[key] = value
                }
            }

            // Apply doc-level metadata
            docMetadata[docSigle]?.forEach { (key, value) ->
                if (!textData.headerMetadata.containsKey(key)) {
                    textData.headerMetadata[key] = value
                }
            }

            val json = generateKrillJson(textData)
            val jsonFileName = textId.replace("_", "-").replace(".", "-") + ".json.gz"

            // Compress JSON with GZIP
            val byteOut = ByteArrayOutputStream()
            val gzipOut = GZIPOutputStream(byteOut)
            gzipOut.write(json.toByteArray(Charsets.UTF_8))
            gzipOut.close()
            val compressedData = byteOut.toByteArray()

            // Write to TAR (synchronized for thread safety)
            synchronized(krillTarOutputStream!!) {
                val tarEntry = TarArchiveEntry(jsonFileName)
                tarEntry.size = compressedData.size.toLong()
                krillTarOutputStream!!.putArchiveEntry(tarEntry)
                krillTarOutputStream!!.write(compressedData)
                krillTarOutputStream!!.closeArchiveEntry()
            }

            val count = krillOutputCount.incrementAndGet()
            LOGGER.fine("Output Krill JSON for $textId ($count total)")

            // Free memory: remove text data from all data structures after successful output
            freeTextMemory(textId)
        } catch (e: Exception) {
            LOGGER.severe("ERROR outputting Krill JSON for $textId: ${e.message}")
            e.printStackTrace()
        }
    }

    // Free memory for a text that has been output
    private fun freeTextMemory(docId: String) {
        texts.remove(docId)
        sentences.remove(docId)
        tokens.remove(docId)
        morpho.remove(docId)
        fnames.remove(docId)
        metadata.remove(docId)
        extraFeatures.remove(docId)
        // krillData is already removed by the caller (either scanner or finalization loop)
        LOGGER.fine("Freed memory for text $docId")
    }

    private fun generateKrillJson(textData: KrillTextData): String {
        val sb = StringBuilder()
        sb.append("{")

        // @context, @type, and version
        sb.append("\"@context\":\"http://korap.ids-mannheim.de/ns/koral/0.4/context.jsonld\",")
        sb.append("\"@type\":\"koral:corpus\",")
        sb.append("\"version\":\"0.4\",")

        // fields (metadata)
        sb.append("\"fields\":[")
        val fields = mutableListOf<String>()

        // Extract corpus, doc, and text sigle from textId (e.g., "WUD24_I0083.95367")
        // Convert underscores to slashes for proper format
        val textIdWithSlashes = textData.textId.replace("_", "/").replace(".", "/")
        val sigleParts = textIdWithSlashes.split("/")
        if (sigleParts.size >= 3) {
            fields.add(jsonObject(listOf(
                "value" to jsonString(sigleParts[0]),
                "type" to jsonString("type:string"),
                "@type" to jsonString("koral:field"),
                "key" to jsonString("corpusSigle")
            )))
            fields.add(jsonObject(listOf(
                "@type" to jsonString("koral:field"),
                "value" to jsonString("${sigleParts[0]}/${sigleParts[1]}"),
                "type" to jsonString("type:string"),
                "key" to jsonString("docSigle")
            )))
            fields.add(jsonObject(listOf(
                "@type" to jsonString("koral:field"),
                "type" to jsonString("type:string"),
                "value" to jsonString(textIdWithSlashes),
                "key" to jsonString("textSigle")
            )))
        }

        // Merge corpus and doc metadata into text metadata (corpus < doc < text precedence)
        val corpusSigle = textIdWithSlashes.split("/")[0]
        val docSigle = textIdWithSlashes.split("/").take(2).joinToString("/")

        // First apply corpus-level metadata (lowest priority)
        corpusMetadata[corpusSigle]?.forEach { (key, value) ->
            if (!textData.headerMetadata.containsKey(key)) {
                textData.headerMetadata[key] = value
            }
        }

        // Then apply doc-level metadata (medium priority)
        docMetadata[docSigle]?.forEach { (key, value) ->
            if (!textData.headerMetadata.containsKey(key)) {
                textData.headerMetadata[key] = value
            }
        }

        // Text-level metadata is already in textData.headerMetadata (highest priority)

        // Add additional metadata fields from header with correct types
        val fieldOrder = listOf(
            "corpusEditor", "distributor", "editor", "translator", "externalLink", "publisher", "reference",
            "creationDate", "pubDate", "textClass", "award", "availability", "language",
            "ISBN", "URN", "pubPlace", "pubPlaceKey",
            "textType", "textTypeArt", "textTypeRef", "textDomain", "textColumn",
            "author", "title", "subTitle", "corpusTitle", "corpusSubTitle", "docTitle"
        )

        fieldOrder.forEach { key ->
            val value = textData.headerMetadata[key] ?: return@forEach

            // Determine field type and value format
            val (fieldType, fieldValue) = when (key) {
                "creationDate", "pubDate" -> {
                    "type:date" to jsonString(value.toString())
                }
                "textClass", "award" -> {
                    "type:keywords" to when (value) {
                        is List<*> -> jsonArray(value.map { jsonString(it.toString()) })
                        else -> jsonArray(listOf(jsonString(value.toString())))
                    }
                }
                "availability", "language", "ISBN", "pubPlace", "pubPlaceKey",
                "textType", "textTypeArt", "textTypeRef", "textDomain", "textColumn" -> {
                    "type:string" to jsonString(value.toString())
                }
                "URN" -> {
                    // URN is stored as a map with urn and url keys
                    when (value) {
                        is Map<*, *> -> {
                            val urn = value["urn"]?.toString() ?: ""
                            val url = value["url"]?.toString() ?: ""
                            val encodedUrn = urn.urlEncode()
                            val encodedUrl = url.urlEncode()
                            "type:attachement" to jsonString("data:application/x.korap-link;title=$encodedUrn,$encodedUrl")
                        }
                        else -> {
                            // Fallback if URN is stored as plain string (shouldn't happen)
                            "type:string" to jsonString(value.toString())
                        }
                    }
                }
                "translator" -> {
                    // Check environment variable for type
                    val translatorAsText = System.getenv("K2K_TRANSLATOR_TEXT")?.isNotEmpty() == true
                    if (translatorAsText) {
                        "type:text" to jsonString(value.toString())
                    } else {
                        "type:attachement" to jsonString("data:,${value.toString()}")
                    }
                }
                "publisher" -> {
                    // Check environment variable for type
                    val publisherAsString = System.getenv("K2K_PUBLISHER_STRING")?.isNotEmpty() == true
                    if (publisherAsString) {
                        "type:string" to jsonString(value.toString())
                    } else {
                        "type:attachement" to jsonString("data:,${value.toString()}")
                    }
                }
                "author", "title", "subTitle", "corpusTitle", "corpusSubTitle", "docTitle" -> {
                    "type:text" to jsonString(value.toString())
                }
                "externalLink" -> {
                    val url = value.toString()
                    // Extract title from corpus/publisher metadata if available
                    val title = textData.headerMetadata["publisher"]?.toString() ?: "Link"
                    val encodedUrl = url.replace(":", "%3A").replace("/", "%2F")
                    "type:attachement" to jsonString("data:application/x.korap-link;title=$title,$encodedUrl")
                }
                else -> {
                    // corpusEditor, distributor, editor, reference - all ATTACHMENT
                    "type:attachement" to jsonString("data:,${value.toString()}")
                }
            }

            fields.add(jsonObject(listOf(
                "key" to jsonString(key),
                "@type" to jsonString("koral:field"),
                "value" to fieldValue,
                "type" to jsonString(fieldType)
            )))
        }

        sb.append(fields.joinToString(","))
        sb.append("],")

        // data section
        sb.append("\"data\":{")
        sb.append("\"text\":${jsonString(textData.textContent ?: "")},")

        val sentenceSpanFoundries = textData.structureSpans.foundriesWithSentenceSpans()
        val constituencySpanFoundries = textData.structureSpans.foundriesWithConstituencySpans()
        val externalSentenceFoundries = sentenceSpanFoundries.filterNot { it in BASE_STRUCTURE_FOUNDRIES }.toSortedSet()
        val externalConstitFoundries = constituencySpanFoundries.filterNot { it in BASE_STRUCTURE_FOUNDRIES }.toSortedSet()
        val layerInfos = mutableListOf<String>()
        if (textData.sentences != null) {
            layerInfos.add("dereko/s=spans")
        }
        externalSentenceFoundries.forEach { layerInfos.add("$it/s=spans") }
        externalConstitFoundries.forEach { layerInfos.add("$it/c=spans") }

        // Collect layers by foundry type (checking what data actually exists)
        val foundryLayers = mutableMapOf<String, MutableSet<String>>()
        textData.morphoByFoundry.keys.sorted().forEach { foundry ->
            val shortFoundry = when(foundry) {
                "base" -> null
                "tree_tagger" -> "tt"
                "marmot-malt" -> "marmot"
                else -> foundry
            }
            if (shortFoundry != null) {
                val layers = foundryLayers.getOrPut(shortFoundry) { mutableSetOf() }
                val morphoData = textData.morphoByFoundry[foundry]?.values

                // Check if this foundry has dependency annotations
                val hasDependencies = morphoData?.any {
                    it.head != null && it.head != "_" && it.deprel != null && it.deprel != "_"
                } ?: false

                if (hasDependencies) {
                    layers.add("d=rels")
                }

                // Check if this foundry has lemma annotations
                val hasLemma = morphoData?.any {
                    it.lemma != null && it.lemma != "_"
                } ?: false
                if (hasLemma) {
                    layers.add("l=tokens")
                }

                // Check if this foundry has POS annotations (xpos or upos)
                val hasPos = morphoData?.any {
                    (it.xpos != null && it.xpos != "_") || (it.upos != null && it.upos != "_")
                } ?: false
                if (hasPos) {
                    layers.add("p=tokens")
                }

                // Check if this foundry has morphological features
                val hasFeatures = morphoData?.any {
                    it.feats != null && it.feats != "_"
                } ?: false
                if (hasFeatures) {
                    layers.add("m=tokens")
                }

                // Check if this foundry has UPOS (skip for tree_tagger)
                if (foundry != "tree_tagger") {
                    val hasUpos = morphoData?.any {
                        it.upos != null && it.upos != "_"
                    } ?: false
                    if (hasUpos) {
                        layers.add("u=tokens")
                    }
                }
            }
        }

        // Add foundry layers in sorted order
        foundryLayers.keys.sorted().forEach { foundry ->
            foundryLayers[foundry]?.sorted()?.forEach { layer ->
                layerInfos.add("$foundry/$layer")
            }
        }
        sb.append("\"layerInfos\":${jsonString(layerInfos.joinToString(" "))},")

        // foundries - list all foundries with their layers
        val foundries = mutableListOf<String>()

        // Add dereko if we have structure
        if (textData.sentences != null) {
            foundries.add("dereko")
            foundries.add("dereko/structure")
            foundries.add("dereko/structure/base-sentences-paragraphs-pagebreaks")
        }

        val advertisedStructureFoundries = (externalSentenceFoundries + externalConstitFoundries).toSortedSet()
        advertisedStructureFoundries.forEach { foundry ->
            if (!foundries.contains(foundry)) {
                foundries.add(foundry)
            }
            if (externalSentenceFoundries.contains(foundry)) {
                val sentencesEntry = "$foundry/sentences"
                if (!foundries.contains(sentencesEntry)) {
                    foundries.add(sentencesEntry)
                }
            }
            if (externalConstitFoundries.contains(foundry)) {
                val structureEntry = "$foundry/structure"
                if (!foundries.contains(structureEntry)) {
                    foundries.add(structureEntry)
                }
            }
        }

        // Add annotation foundries with their layers
        foundryLayers.keys.sorted().forEach { foundry ->
            // Use full name "treetagger" instead of "tt" in foundries list
            val foundryFullName = if (foundry == "tt") "treetagger" else foundry
            foundries.add(foundryFullName)
            foundryLayers[foundry]?.sorted()?.forEach { layer ->
                // Convert layer format: "d=rels" -> "dependency", "p=tokens" -> "morpho", etc.
                val layerName = when {
                    layer.startsWith("d=") -> "dependency"
                    layer.startsWith("l=") || layer.startsWith("p=") || layer.startsWith("m=") || layer.startsWith("u=") -> "morpho"
                    else -> layer.split("=")[0]
                }
                val foundryLayer = "$foundryFullName/$layerName"
                if (!foundries.contains(foundryLayer)) {
                    foundries.add(foundryLayer)
                }
            }
        }
        sb.append("\"foundries\":${jsonString(foundries.joinToString(" "))},")

        // name - field name for the data (always "tokens")
        sb.append("\"name\":\"tokens\",")

        // stream - token-level annotations
        sb.append("\"stream\":[")
        if (textData.tokens != null) {
            val streamItems = generateKrillStream(textData)
            sb.append(streamItems.joinToString(","))
        }
        sb.append("]")

        sb.append("}")  // close data
        sb.append("}")  // close root

        return sb.toString()
    }

    private fun generateKrillStream(textData: KrillTextData): List<String> {
        val rawTokens = textData.tokens ?: return emptyList()
        val text = textData.textContent ?: ""
        val sentences = textData.sentences ?: emptyArray()
        val tokens: List<Span> = if (includeNonWordTokens || text.isEmpty()) {
            rawTokens.toList()
        } else {
            rawTokens.filter { span -> shouldKeepTokenForKrill(text, span) }
        }
        if (tokens.isEmpty()) {
            LOGGER.fine("No tokens remained for ${textData.textId} after filtering non-word tokens")
            return emptyList()
        }
        val result = mutableListOf<String>()

        // Build offset-to-index map for resolving dependency heads and structural spans
        val offsetToIndex = mutableMapOf<String, Int>()
        tokens.forEachIndexed { index, token ->
            offsetToIndex["${token.from}-${token.to}"] = index
        }

        // Collect inverse dependency relations and ROOT dependencies
        data class InverseDep(val dependentIndex: Int, val foundry: String, val deprel: String)
        data class RootDep(val tokenIndex: Int, val foundry: String)
        val inverseDeps = mutableMapOf<Int, MutableList<InverseDep>>()
        val rootTokens = mutableListOf<RootDep>()

        tokens.forEachIndexed { index, token ->
            val spanKey = "${token.from}-${token.to}"
            textData.morphoByFoundry.keys.forEach { foundry ->
                val morphoSpan = textData.morphoByFoundry[foundry]?.get(spanKey)
                if (morphoSpan != null && morphoSpan.head != null && morphoSpan.head != "_" && morphoSpan.deprel != null && morphoSpan.deprel != "_") {
                    val headStr = morphoSpan.head!!
                    val prefix = when(foundry) {
                        "tree_tagger" -> "tt"
                        "marmot-malt" -> "marmot"
                        else -> foundry
                    }

                    // Check if this is a ROOT dependency (head == 0)
                    if (headStr == "0" || (headStr.contains("-") && headStr.startsWith("0-"))) {
                        rootTokens.add(RootDep(index, prefix))
                    } else {
                        val resolvedHeadIndex = if (headStr.contains("-")) {
                            offsetToIndex[headStr]
                        } else {
                            val idx = headStr.toIntOrNull()
                            if (idx != null && idx > 0) idx - 1 else null
                        }

                        if (resolvedHeadIndex != null) {
                            inverseDeps.getOrPut(resolvedHeadIndex) { mutableListOf() }
                                .add(InverseDep(index, prefix, morphoSpan.deprel!!))
                        }
                    }
                }
            }
        }

        // Add base structure spans (sentences, paragraphs, text)
        val baseStructureSpans = mutableListOf<StructureSpan>()

        // Add text span covering entire document (from start of text to end, tokenTo is exclusive)
        if (tokens.isNotEmpty()) {
            baseStructureSpans.add(StructureSpan(
                layer = "base/s:t",
                from = 0,  // Start at beginning of text
                to = tokens.last().to,
                tokenFrom = 0,
                tokenTo = tokens.size,  // Exclusive end: one past last token index
                depth = 0,
                attributes = emptyMap()
            ))
        }

        // Build token-to-sentence map for ROOT edge generation
        data class SentenceInfo(val from: Int, val to: Int, val tokenFrom: Int, val tokenTo: Int)
        val tokenToSentence = mutableMapOf<Int, SentenceInfo>()

        // Add sentence spans (tokenTo is exclusive: first token after the span)
        sentences.forEachIndexed { sentIdx, sentence ->
            val sentTokens = tokens.filter { it.from >= sentence.from && it.to <= sentence.to }
            if (sentTokens.isNotEmpty()) {
                val firstTokenIdx = tokens.indexOf(sentTokens.first())
                val lastTokenIdx = tokens.indexOf(sentTokens.last())
                val sentInfo = SentenceInfo(
                    from = sentTokens.first().from,
                    to = sentTokens.last().to,
                    tokenFrom = firstTokenIdx,
                    tokenTo = lastTokenIdx + 1  // Exclusive end
                )

                // Map all tokens in this sentence to the sentence info
                for (i in firstTokenIdx until sentInfo.tokenTo) {
                    tokenToSentence[i] = sentInfo
                }

                baseStructureSpans.add(StructureSpan(
                    layer = "base/s:s",
                    from = sentInfo.from,
                    to = sentInfo.to,
                    tokenFrom = sentInfo.tokenFrom,
                    tokenTo = sentInfo.tokenTo,
                    depth = 2,
                    attributes = emptyMap()
                ))
            }
        }

        // Combine base structure spans with dereko spans
        val allStructureSpans = baseStructureSpans + textData.structureSpans

        // Resolve tokenFrom and tokenTo for structural spans
        // Note: tokenTo is exclusive (one past the last token index)
        val resolvedStructureSpans = allStructureSpans.map { span ->
            if (span.tokenFrom >= 0 && span.tokenTo >= 0) {
                // Already resolved
                span
            } else {
                // Find first and last token covered by this span
                var tokenFrom = tokens.indexOfFirst { it.from >= span.from && it.from < span.to }
                var lastTokenIndex = tokens.indexOfLast { it.to > span.from && it.to <= span.to }

                // Handle edge cases
                if (tokenFrom == -1) tokenFrom = 0
                if (lastTokenIndex == -1) lastTokenIndex = tokens.size - 1

                // tokenTo is exclusive: one past the last token
                val tokenTo = lastTokenIndex + 1

                span.copy(tokenFrom = tokenFrom, tokenTo = tokenTo)
            }
        }

        val resolvedSentenceFoundries = resolvedStructureSpans.foundriesWithSentenceSpans()
        val resolvedConstitFoundries = resolvedStructureSpans.foundriesWithConstituencySpans()
        val externalSentenceFoundries = resolvedSentenceFoundries.filterNot { it in BASE_STRUCTURE_FOUNDRIES }.toSortedSet()
        val externalConstitFoundries = resolvedConstitFoundries.filterNot { it in BASE_STRUCTURE_FOUNDRIES }.toSortedSet()
        val layerInfos = mutableListOf<String>()
        if (textData.sentences != null) {
            layerInfos.add("dereko/s=spans")
        }
        externalSentenceFoundries.forEach { layerInfos.add("$it/s=spans") }
        externalConstitFoundries.forEach { layerInfos.add("$it/c=spans") }

        // Group structural spans by their starting token
        val spansByToken = mutableMapOf<Int, MutableList<StructureSpan>>()
        resolvedStructureSpans.forEach { span ->
            spansByToken.getOrPut(span.tokenFrom) { mutableListOf() }.add(span)
        }

        // Count paragraph spans (name="p")
        val paragraphCount = allStructureSpans.count { it.layer.endsWith(":p") }
        val sentenceCountsByFoundry = resolvedStructureSpans.sentenceCountsByFoundry()
        val externalSentenceCounts = sentenceCountsByFoundry.entries
            .filter { (foundry, _) -> foundry !in BASE_STRUCTURE_FOUNDRIES }
            .sortedBy { it.key }

        tokens.forEachIndexed { index, token ->
            val tokenAnnotations = mutableListOf<String>()
            val spanKey = "${token.from}-${token.to}"

            // Add counts and structural spans only for first token
            if (index == 0) {
                if (paragraphCount > 0) {
                    tokenAnnotations.add(jsonString("-:base/paragraphs\$<i>$paragraphCount"))
                }
                if (sentences.isNotEmpty()) {
                    tokenAnnotations.add(jsonString("-:base/sentences\$<i>${sentences.size}"))
                }
                externalSentenceCounts.forEach { (foundry, count) ->
                    tokenAnnotations.add(jsonString("-:$foundry/sentences\$<i>$count"))
                }
                tokenAnnotations.add(jsonString("-:tokens\$<i>${tokens.size}"))

                // Add all structural spans that start at token 0 or cover the whole document
                val spansAtZero = spansByToken[0] ?: emptyList()
                spansAtZero.sortedWith(compareBy({ -it.depth }, { it.layer })).forEach { span ->
                    val spanAnnotation = if (span.attributes.isEmpty()) {
                        "<>:${span.layer}\$<b>64<i>${span.from}<i>${span.to}<i>${span.tokenTo}<b>${span.depth}"
                    } else {
                        // Spans with attributes get a unique ID
                        val attrId = span.depth
                        "<>:${span.layer}\$<b>64<i>${span.from}<i>${span.to}<i>${span.tokenTo}<b>${span.depth}<s>$attrId"
                    }
                    tokenAnnotations.add(jsonString(spanAnnotation))

                    // Add attribute annotations
                    span.attributes.forEach { (key, value) ->
                        val attrAnnotation = if (value.isEmpty()) {
                            "@:dereko/s:$key\$<b>17<s>${span.depth}<i>${span.tokenTo}"
                        } else {
                            "@:dereko/s:$key:${value.escapeKrillAttribute()}\$<b>17<s>${span.depth}<i>${span.tokenTo}"
                        }
                        tokenAnnotations.add(jsonString(attrAnnotation))
                    }
                }
            } else {
                // Add structural spans that start at this token
                spansByToken[index]?.sortedWith(compareBy({ -it.depth }, { it.layer }))?.forEach { span ->
                    val spanAnnotation = if (span.attributes.isEmpty()) {
                        "<>:${span.layer}\$<b>64<i>${span.from}<i>${span.to}<i>${span.tokenTo}<b>${span.depth}"
                    } else {
                        "<>:${span.layer}\$<b>64<i>${span.from}<i>${span.to}<i>${span.tokenTo}<b>${span.depth}<s>${span.depth}"
                    }
                    tokenAnnotations.add(jsonString(spanAnnotation))

                    span.attributes.forEach { (key, value) ->
                        val attrAnnotation = if (value.isEmpty()) {
                            "@:dereko/s:$key\$<b>17<s>${span.depth}<i>${span.tokenTo}"
                        } else {
                            "@:dereko/s:$key:${value.escapeKrillAttribute()}\$<b>17<s>${span.depth}<i>${span.tokenTo}"
                        }
                        tokenAnnotations.add(jsonString(attrAnnotation))
                    }
                }
            }

            // Token offset annotation
            tokenAnnotations.add(jsonString("_$index\$<i>${token.from}<i>${token.to}"))

            // Get surface form (used for both i: and s: annotations)
            val surfaceForm = if (token.to <= text.length) {
                text.substring(token.from, token.to)
            } else {
                ""
            }

            // Add i: annotation (lowercase surface form)
            if (surfaceForm.isNotEmpty()) {
                tokenAnnotations.add(jsonString("i:${surfaceForm.lowercase().escapeKrillValue()}"))
            }

            // Add inverse dependency annotations (<:) for dependents pointing to this token as head
            inverseDeps[index]?.sortedBy { "${it.foundry}/${it.deprel}" }?.forEach { inv ->
                tokenAnnotations.add(jsonString("<:${inv.foundry}/d:${inv.deprel.escapeKrillValue()}\$<b>32<i>${inv.dependentIndex}"))
            }

            // Collect annotations from all foundries for this token
            val sortedFoundries = textData.morphoByFoundry.keys.sorted()
            sortedFoundries.forEach { foundry ->
                val morphoSpan = textData.morphoByFoundry[foundry]?.get(spanKey)
                if (morphoSpan != null) {
                    val prefix = when(foundry) {
                        "tree_tagger" -> "tt"
                        "marmot-malt" -> "marmot"
                        "base" -> null  // Skip base for most annotations
                        else -> foundry
                    }

                    if (prefix != null) {
                        // Morphological features (sorted)
                        if (morphoSpan.feats != null && morphoSpan.feats != "_") {
                            val features = mutableListOf<String>()
                            morphoSpan.feats!!.split("|").forEach { feat ->
                                val parts = feat.split("=")
                                if (parts.size == 2) {
                                    val key = parts[0].lowercase().escapeKrillValue()
                                    val value = parts[1].lowercase().escapeKrillValue()
                                    features.add("$prefix/m:$key:$value")
                                }
                            }
                            features.sorted().forEach { tokenAnnotations.add(jsonString(it)) }
                        }

                        // POS (xpos) with optional byte encoding
                        if (morphoSpan.xpos != null && morphoSpan.xpos != "_") {
                            tokenAnnotations.add(jsonString("$prefix/p:${morphoSpan.xpos!!.escapeKrillValue()}"))
                        }

                        // Lemma
                        if (morphoSpan.lemma != null && morphoSpan.lemma != "_") {
                            tokenAnnotations.add(jsonString("$prefix/l:${morphoSpan.lemma!!.escapeKrillValue()}"))
                        }

                        // UPOS (skip for tree_tagger as it only has xpos)
                        if (morphoSpan.upos != null && morphoSpan.upos != "_" && foundry != "tree_tagger") {
                            tokenAnnotations.add(jsonString("$prefix/u:${morphoSpan.upos!!.escapeKrillValue()}"))
                        }
                    }

                    // Dependency relations
                    if (morphoSpan.head != null && morphoSpan.head != "_" && morphoSpan.deprel != null && morphoSpan.deprel != "_") {
                        // Head can be either an offset (e.g., "100-110") or a token index (e.g., "1")
                        val headStr = morphoSpan.head!!
                        val resolvedHeadIndex = if (headStr.contains("-")) {
                            // Offset format - resolve to token index
                            offsetToIndex[headStr]
                        } else {
                            // Already a token index (1-based CoNLL-U format)
                            val idx = headStr.toIntOrNull()
                            if (idx != null && idx > 0) idx - 1 else null  // Convert 1-based to 0-based
                        }

                        if (resolvedHeadIndex != null) {
                            // Regular dependency - outgoing edge to head
                            tokenAnnotations.add(jsonString(">:$prefix/d:${morphoSpan.deprel!!.escapeKrillValue()}\$<b>32<i>$resolvedHeadIndex"))
                        } else if (headStr == "0" || (headStr.contains("-") && headStr.startsWith("0-"))) {
                            // ROOT node - use incoming edge format with full span info
                            tokenAnnotations.add(jsonString("<:$prefix/d:${morphoSpan.deprel!!.escapeKrillValue()}\$<b>34<i>${token.from}<i>${token.to}<i>$index<i>1"))
                        }
                    }
                }
            }

            // Surface form (always last)
            tokenAnnotations.add(jsonString("s:${surfaceForm.escapeKrillValue()}"))

            result.add(jsonArray(tokenAnnotations))
        }

        return result
    }

    private fun shouldKeepTokenForKrill(text: String, span: Span): Boolean {
        if (text.isEmpty()) return true
        val safeFrom = span.from.coerceIn(0, text.length)
        val safeTo = span.to.coerceIn(safeFrom, text.length)
        if (safeFrom >= safeTo) return false
        val surface = text.substring(safeFrom, safeTo)
        return surface.any { it.isLetterOrDigit() || it == '_' }
    }

}  // End of KorapXmlTool class

fun main(args: Array<String>): Unit {
    try { Locale.setDefault(Locale.ROOT) } catch (_: Exception) {}
    exitProcess(CommandLine(KorapXmlTool()).execute(*args))
}

fun debug(args: Array<String>): Int {
    return (CommandLine(KorapXmlTool()).execute(*args))
}

enum class OutputFormat {
    CONLLU, WORD2VEC, KORAPXML, NOW, KRILL
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

object KrillOutputFormat {
    const val NAME = "krill"
}

// JSON utility functions for krill output (no external JSON library dependency)
fun String.escapeJson(): String {
    val sb = StringBuilder()
    for (c in this) {
        when (c) {
            '\\' -> sb.append("\\\\")
            '"' -> sb.append("\\\"")
            '\b' -> sb.append("\\b")
            '\n' -> sb.append("\\n")
            '\r' -> sb.append("\\r")
            '\t' -> sb.append("\\t")
            else -> {
                if (c < ' ') {
                    sb.append(String.format("\\u%04x", c.code))
                } else {
                    sb.append(c)
                }
            }
        }
    }
    return sb.toString()
}

// Escape special characters in Krill attribute values
// These characters have special meaning in the Krill annotation format
fun String.escapeKrillAttribute(): String {
    return this.replace("#", "%23")
        .replace("$", "%24")
        .replace(":", "%3A")
        .replace("<", "%3C")
        .replace(">", "%3E")
}

// Escape special characters in Krill annotation values (POS tags, lemmas, surface forms, etc.)
// The $ character is used as a delimiter in Krill format (e.g., "_1$<i>100")
// The # character is used for offset notation in Krill format
// Both must be percent-encoded when they appear in actual annotation values
fun String.escapeKrillValue(): String {
    // Match legacy korapxml2krill escaping that uses backslashes
    return this.replace("#", "\\#")
        .replace("$", "\\$")
}

fun jsonString(value: String): String = "\"${value.escapeJson()}\""

fun jsonArray(items: List<String>): String = items.joinToString(",", "[", "]")

fun jsonObject(pairs: List<Pair<String, String>>): String {
    return pairs.joinToString(",", "{", "}") { (key, value) ->
        "${jsonString(key)}:${value}"
    }
}

fun String.urlEncode(): String {
    return java.net.URLEncoder.encode(this, "UTF-8")
}
