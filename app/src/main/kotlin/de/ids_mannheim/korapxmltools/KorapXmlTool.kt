package de.ids_mannheim.korapxmltools

import de.ids_mannheim.korapxmltools.AnnotationToolBridgeFactory.Companion.parserFoundries
import de.ids_mannheim.korapxmltools.AnnotationToolBridgeFactory.Companion.taggerFoundries
import de.ids_mannheim.korapxmltools.formatters.KorapXmlFormatter
import de.ids_mannheim.korapxmltools.formatters.KrillJsonGenerator
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
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.ConsoleHandler
import java.util.logging.Level
import java.util.logging.LogManager
import java.util.logging.Logger
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.stream.IntStream
import java.util.zip.GZIPOutputStream
import java.util.zip.ZipFile
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
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.XMLStreamReader

val ZIP_ENTRY_UNIX_MODE = parseInt("644", 8)

@Command(
    name = "korapxmltool",
    mixinStandardHelpOptions = true,
    version = ["korapxmltool v3.1.2"],
    usageHelpAutoWidth = false,
    usageHelpWidth = 200,
    description = ["Converts between KorAP-XML ZIP format and formats like CoNLL-U, Krill, word2vec, NOW\n"+
            "and annotates KorAP XML ZIPs with various taggers and parsers.\n" +
            "Drop-in replacement for korapxml2conllu (https://github.com/KorAP/KorAP-XML-CoNLL-U) and\n" +
            "korapxml2krill (https://github.com/KorAP/KorAP-XML-Krill)\n"],
    footer = ["%nExamples:",
            "  Basic conversion to CoNLL-U format:",
            "    ./build/bin/korapxmltool app/src/test/resources/wdf19.tree_tagger.zip | head -10",
            "",
            "  CoNLL-U to KorAP XML ZIP conversion (auto-detects foundry from comments):",
            "    ./build/bin/conllu2korapxml file.conllu",
            "    cat file.conllu | ./build/bin/conllu2korapxml -o output.zip",
            "    ./build/bin/korapxmltool -t zip -F custom file.conllu",
            "    # Note: Foundry auto-detected from '# foundry = <name>' comment; override with -F",
            "    # Note: Output path auto-inferred (file.conllu → file.zip) or specify with -o",
            "",
            "  Word2Vec style output:",
            "    ./build/bin/korapxmltool -t w2v app/src/test/resources/wud24_sample.zip",
            "",
            "  Extract metadata and convert:",
            "    ./build/bin/korapxmltool -m '<textSigle>([^<]+)' -m '<creatDate>([^<]+)' --word2vec t/data/wdf19.zip",
            "",
            "  NOW corpus export:",
            "    ./build/bin/korapxmltool -t now /vol/corpora/DeReKo/current/KorAP/zip/*24.zip | pv > dach24.txt",
            "",
            "  Tag with integrated MarMot POS tagger, and parse with internal Malt parser:",
            "    ./build/bin/korapxmltool -t zip -T marmot:de.marmot -P malt:german.mco app/src/test/resources/goe.zip",
            "    # (uses KORAPXMLTOOL_MODELS_PATH if model not found in current directory)",
            "",
            "  Native Docker spaCy tagging (without dependencies):",
            "    ./build/bin/korapxmltool -t zip -T spacy app/src/test/resources/goe.zip",
            "",
            "  Native Docker spaCy tagging and dependency parsing:",
            "    ./build/bin/korapxmltool -t zip -P spacy app/src/test/resources/goe.zip",
            "",
            "  Use external spaCy annotation (legacy method):",
            "    ./build/bin/korapxmltool -j4 -A \"docker run -e SPACY_USE_DEPENDENCIES=False --rm -i korap/conllu2spacy:latest\" -t zip ./app/src/test/resources/goe.zip",
            "",
            "  Generate Krill tar from wud24_sample with multiple annotation foundries:",
            "    ./build/bin/korapxmltool -t krill -D . app/src/test/resources/wud24_sample*.zip",
            "",
            "  Large corpus annotation with custom memory and performance and default model settings:",
            "    KORAPXMLTOOL_XMX=500g KORAPXMLTOOL_MODELS_PATH=/data/models KORAPXMLTOOL_JAVA_OPTS=\"-XX:+UseG1GC\" \\",
            "        ./build/bin/korapxmltool -j 100 -t zip -T marmot -P malt wpd25*.zip"
    ]
)

class KorapXmlTool : Callable<Int> {
    val COMPATIBILITY_MODE = System.getenv("COMPATIBILITY_MODE") != null

    @Spec lateinit var spec : Model.CommandSpec

    // When using --annotate-with, hold the external tool's foundry label (e.g., spacy, stanza)
    private var externalFoundry: String? = null
    // Target ZIP filename (when writing ZIP output); used to label the progress bar
    private var targetZipFileName: String? = null
    // Locale is now globally forced to ROOT at startup (see main())

    @Parameters(arity = "0..*", description = ["Input files: KorAP-XML ZIP files or CoNLL-U files (.conllu). If omitted, reads from stdin (requires -o for output path)."])
    var zipFileNames: Array<String>? = null

    @Option(
        names = ["-t", "--to"],
        description = ["Output format: ${ConlluOutputFormat.NAME}, ${Word2VecOutputFormat.NAME}, ${KorapXmlOutputFormat.NAME}, ${NowOutputFormat.NAME}, ${KrillOutputFormat.NAME}",
            "conllu: CoNLL-U format",
            "korapxml, xml, zip: KorAP-XML format zip",
            "word2vec, w2v: Print text in LM training format: tokens separated by space, sentences separated by newlines",
            "now, NOW: NOW corpus export format: w2v-like format with <p> tags for sentence ends and @@<text-sigle> prefix",
            "krill: Krill JSON format (tar file with gzipped JSON files, one per text)"
        ],
        converter = [OutputFormatConverter::class]
    )
    var outputFormat: OutputFormat = OutputFormat.CONLLU
    class OutputFormatConverter : ITypeConverter<OutputFormat> {
        override fun convert(value: String?): OutputFormat {
            return when (value?.lowercase(Locale.getDefault())) {
                "conllu", "conll" -> OutputFormat.CONLLU
                "word2vec", "w2v" -> OutputFormat.WORD2VEC
                "korapxml", "korap", "xml", "zip" -> OutputFormat.KORAP_XML
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

    @Option(
        names = ["--lz4"],
        description = ["Use LZ4 compression for Krill JSON output instead of gzip (faster but larger files)."]
    )
    var useLz4: Boolean = false

    @Option(
        names = ["--log-dir", "-L"],
        paramLabel = "DIR",
        description = ["Directory for the log file (default: same as output file)"]
    )
    var logDir: File? = null

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
        names = ["-j", "--jobs", "--threads"],
        paramLabel = "THREADS",
        description = ["Maximum number of threads to use. Default: intelligent detection based on format and available memory"]
    )
    var maxThreads: Int = 0  // 0 = auto-detect in call()
    fun setThreads(threads: Int) {
        if (threads < 1) {
            throw ParameterException(spec.commandLine(), String.format(Locale.ROOT, "Invalid value `%d' for option '--threads': must be at least 1", threads))
        }
        this.maxThreads = threads
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", threads.toString())
    }


    @Option(
        names = ["--sequential"],
        description = [
            "Process entries inside each zip sequentially; zips processed in parallel (only for word2vec/now)."
        ]
    )
    var sequentialInZip: Boolean = false

    @Option(
        names = ["-f", "--force"],
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
        names = ["-o", "--output"],
        paramLabel = "FILE",
        description = ["Output file path. Default: <base input>.krill for krill format, <base input>.<foundry>.zip for KorAP-XML ZIP, and stdout for conllu, now and w2v format."]
    )
    var outputFile: String? = null

    @Option(
        names = ["-F", "--foundry"],
        paramLabel = "FOUNDRY",
        description = ["Override foundry name for CoNLL-U input (default: auto-detect from '# foundry = <name>' comment)"]
    )
    var foundryOverride: String? = null

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
            "Only valid with -t word2vec or -t now; implies --lemma."
        ]
    )
    var lemmaOnly: Boolean = false

    private var taggerName: String? = null
    private var taggerModel: String? = null
    private var dockerLogMessage: String? = null
    
    // Store model path resolutions for logging after logger initialization
    private val modelPathResolutions: MutableList<Pair<String, String>> = mutableListOf()
    
    // Default models for taggers and parsers
    private val defaultTaggerModels = mapOf(
        "marmot" to "de.marmot",
        "opennlp" to "de-pos-maxent.bin",
        "corenlp" to "german-fast.tagger",
        "treetagger" to "german"
    )

    data class DockerTaggerConfig(val image: String, val defaultModel: String, val defaultArgs: String)
    private val dockerTaggers = mapOf(
        "treetagger" to DockerTaggerConfig("korap/conllu-treetagger", "german", "-p"),
        "spacy" to DockerTaggerConfig("korap/conllu-spacy", "de_core_news_lg", "")
    )

    private val defaultParserModels = mapOf(
        "malt" to "german.mco",
        "corenlp" to "germanSR.ser.gz",
        "spacy" to "de_core_news_lg"
    )

    // Calculate optimal thread count based on format, memory, and input characteristics
    private fun calculateOptimalThreads(): Int {
        val availableCores = Runtime.getRuntime().availableProcessors()
        val availableMemoryGB = getAvailableMemoryGB()
        
        // Detect program name for compatibility mode
        val programName = System.getProperty("sun.java.command")?.split(" ")?.first()?.split("/")?.last() ?: "korapxmltool"
        
        return when {
            // CoNLL-U conversion: lightweight, max 10 threads
            programName == "korapxml2conllu" || outputFormat == OutputFormat.CONLLU -> {
                minOf(10, availableCores)
            }
            
            // Krill export: depends on corpus size
            programName == "korapxml2krill" || outputFormat == OutputFormat.KRILL -> {
                calculateKrillThreads(availableCores, availableMemoryGB)
            }
            
            // Tagging/Parsing: memory-intensive, needs careful balancing
            taggerName != null || parserName != null -> {
                calculateAnnotationThreads(availableCores, availableMemoryGB)
            }
            
            // Default case: conservative threading for general processing
            else -> {
                minOf(availableCores / 2, 8)
            }
        }.also { threads ->
            LOGGER.info("Thread calculation: format=$outputFormat, cores=$availableCores, memory=${availableMemoryGB}GB, result=$threads")
        }
    }
    
    private fun calculateKrillThreads(cores: Int, memoryGB: Double): Int {
        // Analyze input corpus size if possible
        val totalInputSize = zipFileNames?.sumOf { File(it).length() } ?: 0L
        val totalInputGB = totalInputSize / (1024.0 * 1024.0 * 1024.0)
        
        return when {
            // Very large corpora (>30GB): scale with cores, up to 75% utilization
            totalInputGB > 30 -> {
                minOf(cores, maxOf(32, cores * 3 / 4))
            }
            // Large corpora (>20GB): scale based on size and available cores
            totalInputGB > 20 -> {
                minOf(cores, maxOf(16, cores * 3 / 4))
            }
            // Medium-large corpora (10-20GB): scale threads with input size and cores
            totalInputGB > 10 -> {
                // For large machines (64+ cores), scale aggressively; medium machines (32+) moderately
                when {
                    cores >= 64 -> minOf(cores * 3 / 4, maxOf(16, (totalInputGB * 1.2).toInt()))
                    cores >= 32 -> minOf(cores / 2, maxOf(10, (totalInputGB * 0.8).toInt()))
                    else -> minOf(cores, 16)
                }
            }
            // Medium corpora (1-10GB): 10 threads on small machines, scale on large machines
            totalInputGB > 1 -> {
                when {
                    cores >= 64 -> minOf(cores / 2, 32)
                    cores >= 32 -> minOf(cores / 4, 16)
                    else -> minOf(cores, 10)
                }
            }
            // Small-medium corpora (0.1-1GB): use 8 threads  
            totalInputGB > 0.1 -> {
                minOf(cores, 8)
            }
            // Very small corpora (<0.1GB): minimal threading + overhead for compression/writing
            else -> {
                minOf(4, cores / 2)
            }
        }.also { threads ->
            LOGGER.info("Krill threading: input=${"%.1f".format(totalInputGB)}GB, cores=$cores, threads=$threads")
        }
    }
    
    private fun calculateAnnotationThreads(cores: Int, memoryGB: Double): Int {
        // Annotation tools are very memory-intensive (especially parsers)
        // Estimate memory per thread: parsing ~1.5GB, tagging ~0.8GB
        val memoryPerThreadGB = when {
            parserName != null -> 1.5  // 1.5GB per parser thread
            taggerName != null -> 0.8  // 0.8GB per tagger thread  
            else -> 0.5                // 0.5GB per thread for other operations
        }
        
        val memoryBasedThreads = maxOf(1, ((memoryGB * 0.8) / memoryPerThreadGB).toInt())
        val coreBasedThreads = maxOf(1, cores / 2)  // Leave cores for I/O and GC
        
        return minOf(memoryBasedThreads, coreBasedThreads, 16).also { threads ->
            LOGGER.info("Annotation threading: ${parserName ?: taggerName}, memory=${"%.1f".format(memoryGB)}GB, " +
                       "memLimit=${memoryBasedThreads}t, coreLimit=${coreBasedThreads}t, result=${threads}t")
        }
    }
    
    private fun getAvailableMemoryGB(): Double {
        // Get heap size that was actually allocated by JVM
        val runtime = Runtime.getRuntime()
        val maxMemory = runtime.maxMemory()
        
        // Convert to GB, use 80% for safety (leave room for GC, off-heap, etc.)
        return maxMemory * 0.8 / (1024.0 * 1024.0 * 1024.0)
    }

    // Helper function to resolve model path with default search directory
    private fun resolveModelPath(modelPath: String): String? {
        // If absolute path or relative path exists as-is, return it
        if (File(modelPath).exists()) {
            return modelPath
        }
        
        // Check if KORAPXMLTOOL_MODELS_PATH environment variable is set
        val defaultModelsPath = System.getenv("KORAPXMLTOOL_MODELS_PATH")
        if (!defaultModelsPath.isNullOrBlank()) {
            val resolvedPath = File(defaultModelsPath, modelPath).absolutePath
            if (File(resolvedPath).exists()) {
                return resolvedPath
            }
            
            // If modelPath contains directory separators, try with just the filename
            val fileName = File(modelPath).name
            if (fileName != modelPath) {
                val fileNamePath = File(defaultModelsPath, fileName).absolutePath
                if (File(fileNamePath).exists()) {
                    return fileNamePath
                }
            }
        }
        
        // Model not found in any location
        return null
    }
    @Option(
        names = ["-T", "--tag-with"],
        paramLabel = "TAGGER[:MODEL]",
        description = ["Specify a tagger and optionally a model: ${taggerFoundries}[:<path/to/model>].",
                      "If model is omitted, defaults are: marmot→de.marmot, opennlp→de-pos-maxent.bin, corenlp→german-fast.tagger, treetagger→german, spacy→de_core_news_lg"]
    )
    fun setTagWith(tagWith: String) {
        // Pattern now makes the model part optional
        val pattern: Pattern = Pattern.compile("(${taggerFoundries})(?::(.+))?")
        val matcher: Matcher = pattern.matcher(tagWith)
        if (!matcher.matches()) {
            throw ParameterException(spec.commandLine(),
                String.format(Locale.ROOT, "Invalid value `%s' for option '--tag-with': "+
                    "value does not match the expected pattern ${taggerFoundries}[:<path/to/model>]", tagWith))
        } else {

            taggerName = matcher.group(1)
            
            if (dockerTaggers.containsKey(taggerName)) {
                val config = dockerTaggers[taggerName]!!
                val modelPart = matcher.group(2)
                
                var model = config.defaultModel
                var args = config.defaultArgs
                
                if (modelPart != null) {
                    // Split by first colon to separate model and args if present
                    // Format could be: "model" or "model:args" or ":args" (if model is empty, but regex group 2 implies presence)
                    // Actually regex is (foundry)(?::(.+))? so group 2 is everything after first colon
                    // We want to support:
                    // treetagger -> default model, default args
                    // treetagger:german -> german model, default args
                    // treetagger:german:-p -x -> german model, custom args
                    
                    val parts = modelPart.split(":", limit = 2)
                    if (parts.isNotEmpty() && parts[0].isNotBlank()) {
                        model = parts[0]
                    }
                    if (parts.size > 1) {
                        val customArgs = parts[1]
                        args = if (config.defaultArgs.isNotBlank()) {
                            "${config.defaultArgs} $customArgs"
                        } else {
                            customArgs
                        }
                    }
                }
                
                taggerModel = model // For logging
                
                // Construct Docker command
                // We assume KORAPXMLTOOL_MODELS_PATH is set in the environment or we use a default?
                // The user request said: "docker run -v $KORAPXMLTOOL_MODELS_PATH:/local/models ..."
                // AnnotationWorkerPool uses /bin/sh -c, so environment variables should be expanded by the shell.
                
                // Handle different Docker command formats
                if (taggerName == "spacy") {
                    // spaCy uses -m for model and -d to disable dependencies (tagging only)
                    annotateWith = "docker run -v \${KORAPXMLTOOL_MODELS_PATH:-.}:/local/models --rm -i ${config.image} -m $model -d"
                } else if (taggerName == "treetagger") {
                    // TreeTagger uses -l for language/model and -p in args
                    annotateWith = "docker run -v \${KORAPXMLTOOL_MODELS_PATH:-.}:/local/models --rm -i ${config.image} $args -l $model"
                } else {
                    annotateWith = "docker run -v \${KORAPXMLTOOL_MODELS_PATH:-.}:/local/models --rm -i ${config.image} $args $model"
                }
                dockerLogMessage = "Configured Docker tagger '$taggerName' with command: $annotateWith"
                
            } else {
                val originalModelPath = matcher.group(2) ?: defaultTaggerModels[taggerName]
    
                if (originalModelPath == null) {
                    throw ParameterException(spec.commandLine(),
                        String.format(Locale.ROOT, "No default model available for tagger '%s'", taggerName))
                }
    
                val resolvedModelPath = resolveModelPath(originalModelPath)
                
                if (resolvedModelPath != null) {
                    taggerModel = resolvedModelPath
                    if (resolvedModelPath != originalModelPath) {
                        // Store for logging after logger initialization
                        modelPathResolutions.add(originalModelPath to resolvedModelPath)
                    }
                } else {
                    val defaultModelsPath = System.getenv("KORAPXMLTOOL_MODELS_PATH")
                    val searchInfo = if (defaultModelsPath != null) {
                        " (searched in current directory and KORAPXMLTOOL_MODELS_PATH='$defaultModelsPath')"
                    } else {
                        " (searched in current directory; KORAPXMLTOOL_MODELS_PATH defaults to ../lib/models relative to executable)"
                    }
                    throw ParameterException(spec.commandLine(),
                        String.format(Locale.ROOT, "Invalid value for option '--tag-with': "+
                            "model file '%s' does not exist%s", originalModelPath, searchInfo))
                }
            }
        }
    }

    private var parserName: String? = null
    private var parserModel: String? = null
    @Option(
        names = ["-P", "--parse-with"],
        paramLabel = "PARSER[:MODEL]",
        description = ["Specify a parser and optionally a model: ${parserFoundries}[:<path/to/model>].",
                      "If model is omitted, defaults are: malt→german.mco, corenlp→germanSR.ser.gz, spacy→de_core_news_lg"]
    )
    fun setParseWith(parseWith: String) {
        // Pattern now makes the model part optional
        val pattern: Pattern = Pattern.compile("(${parserFoundries})(?::(.+))?")
        val matcher: Matcher = pattern.matcher(parseWith)
        if (!matcher.matches()) {
            throw ParameterException(spec.commandLine(),
                String.format(Locale.ROOT, "Invalid value `%s' for option '--parse-with': "+
                        "value does not match the expected pattern ${parserFoundries}[:<path/to/model>]", parseWith))
        } else {
            parserName = matcher.group(1)
            
            // Handle Docker parsers (like spaCy)
            if (dockerTaggers.containsKey(parserName)) {
                val config = dockerTaggers[parserName]!!
                val modelPart = matcher.group(2)
                
                var model = config.defaultModel
                var args = config.defaultArgs
                
                if (modelPart != null) {
                    val parts = modelPart.split(":", limit = 2)
                    if (parts.isNotEmpty() && parts[0].isNotBlank()) {
                        model = parts[0]
                    }
                    if (parts.size > 1) {
                        val customArgs = parts[1]
                        args = if (config.defaultArgs.isNotBlank()) {
                            "${config.defaultArgs} $customArgs"
                        } else {
                            customArgs
                        }
                    }
                }
                
                parserModel = model // For logging
                
                // For spaCy parsing, do NOT add -d flag (parsing is enabled by default)
                if (parserName == "spacy") {
                    // spaCy uses -m for model, no -d flag for parsing mode
                    annotateWith = "docker run -v \${KORAPXMLTOOL_MODELS_PATH:-.}:/local/models --rm -i ${config.image} -m $model"
                } else {
                    annotateWith = "docker run -v \${KORAPXMLTOOL_MODELS_PATH:-.}:/local/models --rm -i ${config.image} $args $model"
                }
                dockerLogMessage = "Configured Docker parser '$parserName' with command: $annotateWith"
                
            } else {
                val originalModelPath = matcher.group(2) ?: defaultParserModels[parserName]

                if (originalModelPath == null) {
                    throw ParameterException(spec.commandLine(),
                        String.format(Locale.ROOT, "No default model available for parser '%s'", parserName))
                }

                val resolvedModelPath = resolveModelPath(originalModelPath)
                
                if (resolvedModelPath != null) {
                    parserModel = resolvedModelPath
                    if (resolvedModelPath != originalModelPath) {
                        // Store for logging after logger initialization
                        modelPathResolutions.add(originalModelPath to resolvedModelPath)
                    }
                } else {
                    val defaultModelsPath = System.getenv("KORAPXMLTOOL_MODELS_PATH")
                    val searchInfo = if (defaultModelsPath != null) {
                        " (searched in current directory and KORAPXMLTOOL_MODELS_PATH='$defaultModelsPath')"
                    } else {
                        " (searched in current directory; KORAPXMLTOOL_MODELS_PATH defaults to ../lib/models relative to executable)"
                    }
                    throw ParameterException(spec.commandLine(),
                        String.format(Locale.ROOT, "Invalid value for option '--parse-with': "+
                                "model file '%s' does not exist%s", originalModelPath, searchInfo))
                }
            }
        }
    }


    override fun call(): Int {
        val handler = ConsoleHandler()
        LogManager.getLogManager().reset()
        handler.formatter = ColoredFormatter()

        if (System.getProperty("korapxmltool.test") == "true") {
            quiet = true
        }

        for (handler in LOGGER.handlers) {
            LOGGER.removeHandler(handler)
        }
        LOGGER.addHandler(handler)
        val level = try {
            Level.parse(logLevel.uppercase(Locale.getDefault()))
        } catch (e: IllegalArgumentException) {
            LOGGER.warning("Invalid log level: $logLevel. Defaulting to WARNING.")
            Level.WARNING
        }
        LOGGER.level = level
        handler.level = level  // Handler also needs to be set to the same level

        // Auto-detect optimal thread count if not specified
        if (maxThreads == 0) {
            maxThreads = calculateOptimalThreads()
            LOGGER.info("Auto-detected optimal thread count: $maxThreads threads")
        }

        // Log model path resolutions that occurred during parameter parsing
        modelPathResolutions.forEach { (original, resolved) ->
            LOGGER.info("Resolved model path '$original' to '$resolved'")
        }
        
        // Validate input files exist before doing any processing
        zipFileNames?.forEach { zipFile ->
            if (!File(zipFile).exists()) {
                System.err.println("ERROR: Input file does not exist: $zipFile")
                return 1
            }
            if (!File(zipFile).canRead()) {
                System.err.println("ERROR: Cannot read input file: $zipFile")
                return 1
            }
        }

        if (lemmaOnly) {
            useLemma = true
            if (outputFormat != OutputFormat.WORD2VEC && outputFormat != OutputFormat.NOW) {
                throw ParameterException(spec.commandLine(), "--lemma-only is supported only with -t word2vec or -t now")
            }
        }

        // For krill format, redirect logging to file before any logging occurs
        if (outputFormat == OutputFormat.KRILL) {
            // Determine output path for Krill format
            krillOutputPath = if (outputFile != null) {
                // Use explicit -o option - has highest priority, use as-is
                val finalOutputPath = outputFile!!
                // Ensure .tar extension for Krill format
                if (finalOutputPath.endsWith(".tar")) {
                    finalOutputPath
                } else {
                    "$finalOutputPath.tar"
                }
            } else {
                // Find the base ZIP (one without a foundry suffix)
                val baseZip = zipFileNames!!.firstOrNull { zip ->
                    val name = File(zip).name
                    name.matches(Regex(".*\\.zip$")) && !name.matches(Regex(".*\\.[^/.]+\\.zip$"))
                } ?: zipFileNames!![0]

                val baseZipName = File(baseZip).name.replace(Regex("\\.zip$"), "")
                File(outputDir, "$baseZipName.krill.tar").absolutePath
            }
            var logFilePath = krillOutputPath!!.replace(Regex("\\.tar$"), ".log")
            
            if (logDir != null) {
                if (!logDir!!.exists()) {
                     logDir!!.mkdirs()
                }
                logFilePath = File(logDir, File(logFilePath).name).absolutePath
            }

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

        // CoNLL-U to KorAP XML ZIP conversion mode
        val isConlluInput = zipFileNames == null || zipFileNames!!.isEmpty() || 
                           zipFileNames!!.any { it.endsWith(".conllu") }
        
        if (isConlluInput) {
            // Validate: CoNLL-U mode requires -t zip (default or explicit)
            if (outputFormat != OutputFormat.KORAP_XML) {
                throw ParameterException(spec.commandLine(), 
                    "CoNLL-U input requires output format 'zip' (use -t zip or invoke as 'conllu2korapxml')")
            }

            when {
                // Case 1: stdin input (no files specified)
                zipFileNames == null || zipFileNames!!.isEmpty() -> {
                    if (outputFile == null) {
                        throw ParameterException(spec.commandLine(),
                            "Reading from stdin requires -o/--output to specify output file path")
                    }
                    // -o has highest priority - use it as-is (absolute or relative to CWD)
                    val finalOutputPath = outputFile!!
                    LOGGER.info("Converting CoNLL-U from stdin to: $finalOutputPath")
                    convertConlluToZip(System.`in`, finalOutputPath)
                    return 0
                }
                
                // Case 2: CoNLL-U file(s) specified
                zipFileNames!!.all { it.endsWith(".conllu") } -> {
                    zipFileNames!!.forEach { conlluFile ->
                        val outputPath = when {
                            outputFile != null -> {
                                // -o has highest priority - use it as-is (absolute or relative to CWD)
                                outputFile!!
                            }
                            else -> {
                                // Auto-infer from input filename
                                val baseName = File(conlluFile).name.replace(Regex("\\.conllu$"), ".zip")
                                if (outputDir != ".") {
                                    File(outputDir, baseName).path
                                } else {
                                    conlluFile.replace(Regex("\\.conllu$"), ".zip")
                                }
                            }
                        }
                        LOGGER.info("Converting CoNLL-U file: $conlluFile → $outputPath")
                        FileInputStream(conlluFile).use { inputStream ->
                            convertConlluToZip(inputStream, outputPath)
                        }
                    }
                    return 0
                }
                
                // Case 3: Mixed input (some .conllu, some .zip) - not supported
                else -> {
                    throw ParameterException(spec.commandLine(),
                        "Cannot mix CoNLL-U (.conllu) and ZIP files in the same invocation")
                }
            }
        }

        // Normal ZIP processing mode
        if (outputFile != null && (outputFormat == OutputFormat.CONLLU || outputFormat == OutputFormat.WORD2VEC || outputFormat == OutputFormat.NOW)) {
            // -o has highest priority - use it as-is (absolute or relative to CWD)
            val finalOutputPath = outputFile!!
            val file = File(finalOutputPath)
            if (file.exists()) {
                if (!overwrite) {
                    System.err.println("ERROR: Output file $finalOutputPath already exists. Use --force to overwrite.")
                    return 1
                }
                file.delete()
            }
            // Create parent directories
            file.parentFile?.mkdirs()
            
            // Initialize output writer with optional gzip compression
            val outputStream = FileOutputStream(file)
            textOutputWriter = if (finalOutputPath.endsWith(".gz")) {
                BufferedWriter(OutputStreamWriter(GZIPOutputStream(outputStream), StandardCharsets.UTF_8))
            } else {
                BufferedWriter(OutputStreamWriter(outputStream, StandardCharsets.UTF_8))
            }
        }

        LOGGER.info("Processing zip files: " + zipFileNames!!.joinToString(", "))

        korapxml2conllu(zipFileNames!!)
        return 0
    }

    private val LOGGER: Logger = Logger.getLogger(KorapXmlTool::class.java.name)

    // Helper function to write output to file or stdout
    private fun writeOutput(content: String) {
        if (textOutputWriter != null) {
            textOutputWriter!!.write(content)
            textOutputWriter!!.newLine()  // Add newline to match println() behavior
        } else {
            println(content)
        }
    }

    private var annotationWorkerPool : AnnotationWorkerPool? = null

    // Track the next text ID (watermark) each foundry needs to process for priority scheduling
    // The foundry with the lexicographically smallest next text ID gets priority
    private val foundryWatermarks: ConcurrentHashMap<String, String> = ConcurrentHashMap()
    private var scanOrderLogged = false
    private var expectedTextOrder: List<String> = emptyList()
    private var nextTextOrderIndex: Int = 0
    
    // Work-stealing scheduler for Krill output: maintains queues per foundry
    private val foundryTaskQueues: ConcurrentHashMap<String, java.util.concurrent.ConcurrentLinkedQueue<PrioritizedTask>> = ConcurrentHashMap()
    private val foundryTaskCounts: ConcurrentHashMap<String, AtomicInteger> = ConcurrentHashMap()
    private val foundrySubmissionComplete: ConcurrentHashMap<String, Boolean> = ConcurrentHashMap()
    private var workStealingSchedulerActive = false
    @Volatile private var allFoundriesSubmitted = false
    
    // Track which foundries have completed each text for incremental output
    private val textFoundryCompletion: ConcurrentHashMap<String, MutableSet<String>> = ConcurrentHashMap()
    private val expectedFoundriesPerText: ConcurrentHashMap<String, Set<String>> = ConcurrentHashMap()

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
            val textIdDiff = compareTextIds(textId, other.textId)
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

    private val MONTH_ORDER = mapOf(
        "JAN" to 1, "FEB" to 2, "MAR" to 3, "MRZ" to 3, "APR" to 4,
        "MAY" to 5, "MAI" to 5, "JUN" to 6, "JUL" to 7, "AUG" to 8,
        "SEP" to 9, "OCT" to 10, "OKT" to 10, "NOV" to 11, "DEC" to 12, "DEZ" to 12
    )

    private data class TextIdSortKey(
        val prefix: String,
        val monthRank: Int?,
        val mid: String,
        val number: Long,
        val fallback: String
    ) : Comparable<TextIdSortKey> {
        override fun compareTo(other: TextIdSortKey): Int {
            // First compare by prefix
            val prefixCmp = prefix.compareTo(other.prefix)
            if (prefixCmp != 0) return prefixCmp

            // Then compare by month rank (if both have months, use rank; otherwise fall back to mid)
            val thisRank = monthRank ?: Int.MAX_VALUE
            val otherRank = other.monthRank ?: Int.MAX_VALUE
            val rankCmp = thisRank.compareTo(otherRank)
            if (rankCmp != 0) return rankCmp

            // If both have no month rank (both MAX_VALUE), compare mid alphabetically
            if (monthRank == null && other.monthRank == null) {
                val midCmp = mid.compareTo(other.mid)
                if (midCmp != 0) return midCmp
            }

            // Then compare by number
            val numberCmp = number.compareTo(other.number)
            if (numberCmp != 0) return numberCmp

            // Finally fallback to full ID
            return fallback.compareTo(other.fallback)
        }
    }

    // Extract text ID from ZIP entry path (e.g., "ZGE24/JAN/00001/base/data.xml" -> "ZGE24_JAN.00001")
    private fun getTextIdFromPath(path: String): String {
        val parts = path.split('/')
        return if (parts.size >= 3) {
            "${parts[0]}_${parts[1]}.${parts[2]}"
        } else {
            parts[0]  // Fallback to first component
        }
    }

    private fun monthAwareSortKey(textId: String): TextIdSortKey {
        val parts = Regex("[-_.]").split(textId)
        val prefix = parts.getOrNull(0) ?: textId
        val mid = parts.getOrNull(1) ?: ""
        val tailNumber = parts.getOrNull(2)?.toLongOrNull() ?: Long.MAX_VALUE
        val monthRank = if (mid.length == 3) MONTH_ORDER[mid.uppercase(Locale.ROOT)] else null
        return TextIdSortKey(prefix, monthRank, mid, tailNumber, textId)
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
    // Output stream for the "morpho" layer ZIP entries
    internal var morphoZipOutputStream: ZipArchiveOutputStream? = null
    var krillTarOutputStream: TarArchiveOutputStream? = null
    var krillOutputFileName: String? = null
    private var krillOutputPath: String? = null
    private var textOutputWriter: BufferedWriter? = null

    // Fast DocumentBuilderFactory without security features (safe for trusted input)
    private val fastDomFactory: DocumentBuilderFactory by lazy {
        DocumentBuilderFactory.newInstance().apply {
            isNamespaceAware = false
            isValidating = false
            // Disable expensive security features for performance (corpus XML is trusted)
            trySetFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
            trySetFeature("http://xml.org/sax/features/external-general-entities", false)
            trySetFeature("http://xml.org/sax/features/external-parameter-entities", false)
        }
    }

    // Thread-local DocumentBuilder pool for parallel processing
    private val threadLocalBuilder: ThreadLocal<DocumentBuilder> = ThreadLocal.withInitial {
        fastDomFactory.newDocumentBuilder()
    }

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

    // Thread-local XMLInputFactory for StAX parsing
    private val xmlInputFactory: ThreadLocal<XMLInputFactory> = ThreadLocal.withInitial {
        XMLInputFactory.newInstance().apply {
            setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
            setProperty(XMLInputFactory.IS_VALIDATING, false)
            setProperty(XMLInputFactory.SUPPORT_DTD, false)
            setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
            setProperty(XMLInputFactory.IS_COALESCING, false)
        }
    }

    // Data class to hold compressed Krill JSON ready for TAR writing
    data class CompressedKrillData(
        val textId: String,
        val fileName: String,
        val compressedBytes: ByteArray
    )

    val krillData: ConcurrentHashMap<String, KrillJsonGenerator.KrillTextData> = ConcurrentHashMap()
    val krillCompressedData: ConcurrentHashMap<String, CompressedKrillData> = ConcurrentHashMap()
    val krillCompressionFutures: ConcurrentHashMap<String, java.util.concurrent.Future<*>> = ConcurrentHashMap()
    val corpusMetadata: ConcurrentHashMap<String, MutableMap<String, Any>> = ConcurrentHashMap()
    val docMetadata: ConcurrentHashMap<String, MutableMap<String, Any>> = ConcurrentHashMap()
    val expectedFoundries: MutableSet<String> = mutableSetOf("base")
    val processedFoundries: MutableSet<String> = mutableSetOf()
    var krillOutputCount = java.util.concurrent.atomic.AtomicInteger(0)

    // Compression thread pool for parallel GZIP compression
    var compressionExecutor: java.util.concurrent.ExecutorService? = null

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
        // For Krill output, use work-stealing scheduler for optimal core utilization
        try {
        if (outputFormat == OutputFormat.KRILL) {
            workStealingSchedulerActive = true
            allFoundriesSubmitted = false
            entryExecutor = java.util.concurrent.Executors.newFixedThreadPool(maxThreads) { r ->
                Thread(r, "KrillWorker-${Thread.currentThread().threadId()}")
            }
            LOGGER.info("Initialized work-stealing scheduler with $maxThreads worker threads for Krill output")
        } else {
            // For other formats, use priority-based executor
            entryExecutor = java.util.concurrent.ThreadPoolExecutor(
                maxThreads,
                maxThreads,
                0L,
                java.util.concurrent.TimeUnit.MILLISECONDS,
                priorityQueue
            )
            LOGGER.info("Initialized watermark-based entry executor with $maxThreads threads (foundries scheduled by text ID to progress together)")
        }

        // Initialize TAR output for krill format
        if (outputFormat == OutputFormat.KRILL) {
            // Find the base ZIP (one without a foundry suffix)
            val baseZip = args.firstOrNull { zip ->
                val name = File(zip).name
                name.matches(Regex(".*\\.zip$")) && !name.matches(Regex(".*\\.[^/.]+\\.zip$"))
            } ?: args[0]

            val baseZipName = File(baseZip).name.replace(Regex("\\.zip$"), "")
            krillOutputFileName = krillOutputPath ?: File(outputDir, "$baseZipName.krill.tar").absolutePath
            LOGGER.info("Initializing krill TAR output: $krillOutputFileName")

            if (File(krillOutputFileName!!).exists() && !overwrite) {
                LOGGER.severe("Output file $krillOutputFileName already exists. Use --force to overwrite.")
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

            // Note: Progress bar and incremental writer will be initialized in processZipsInterleavedForKrill
            // after scanning ZIPs (to avoid double scanning)
        }

        if (annotateWith.isNotEmpty()) {
            // Detect external foundry label once from annotateWith command
            externalFoundry = foundryOverride ?: detectFoundryFromAnnotateCmd(annotateWith)
            
            // Declare logFilePath outside the block so it's accessible later
            var logFilePath: String? = null
            
            // Initialize ZIP output stream BEFORE creating worker pool, if needed
            if (outputFormat == OutputFormat.KORAP_XML) {
                // Determine output filename - respect outputDir consistently
                val inputZipPath = args[0] // First ZIP file
                val targetFoundry = externalFoundry ?: "annotated"

                val baseZipName = File(inputZipPath).name.replace(Regex("\\.zip$"), "")
                val autoOutputFileName = File(outputDir, "$baseZipName.$targetFoundry.zip").absolutePath
                val outputMorphoZipFileName = outputFile ?: autoOutputFileName
                targetZipFileName = outputMorphoZipFileName

                // Check for existing output file BEFORE redirecting logging, so user sees the message
                if (File(outputMorphoZipFileName).exists() && !overwrite) {
                    val errorMsg = "Output file $outputMorphoZipFileName already exists. Use --force to overwrite."
                    System.err.println("ERROR: $errorMsg")
                    LOGGER.severe(errorMsg)
                    exitProcess(1)
                }

                LOGGER.info("Initializing output ZIP: $outputMorphoZipFileName (from input: $inputZipPath, foundry: $targetFoundry)")
                // Prepare per-output log file
                logFilePath = outputMorphoZipFileName.replace(Regex("\\.zip$"), ".log")
                
                if (logDir != null) {
                    if (!logDir!!.exists()) {
                         logDir!!.mkdirs()
                    }
                    logFilePath = File(logDir, File(logFilePath).name).absolutePath
                }

                if (File(logFilePath).parentFile?.exists() == false) {
                     System.err.println("Error: Output directory '${File(logFilePath).parentFile}' does not exist.")
                     exitProcess(1)
                }
                val fileHandler = java.util.logging.FileHandler(logFilePath, true)
                fileHandler.formatter = ColoredFormatter()
                
                // Remove existing console handlers so logs only go to file
                for (logHandler in LOGGER.handlers.toList()) {
                    LOGGER.removeHandler(logHandler)
                }
                
                val rootLogger = java.util.logging.Logger.getLogger("")
                for (handler in rootLogger.handlers) {
                    if (handler is java.util.logging.ConsoleHandler) {
                        rootLogger.removeHandler(handler)
                    }
                }
                
                LOGGER.addHandler(fileHandler)
                LOGGER.info("Logging redirected to: $logFilePath")
                // Mirror System.err to the same log file for the duration
                val errPs = java.io.PrintStream(java.io.BufferedOutputStream(java.io.FileOutputStream(logFilePath, true)), true)
                val oldErr = System.err
                System.setErr(errPs)

                // Delete old file if it exists
                if (File(outputMorphoZipFileName).exists()) {
                    LOGGER.info("Deleting existing file: $outputMorphoZipFileName")
                    File(outputMorphoZipFileName).delete()
                }

                dbFactory = DocumentBuilderFactory.newInstance()
                dBuilder = dbFactory!!.newDocumentBuilder()
                File(outputMorphoZipFileName).parentFile?.mkdirs()
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

            if (outputFormat == OutputFormat.KORAP_XML) {
                annotationWorkerPool = AnnotationWorkerPool(annotateWith, maxThreads, LOGGER, { annotatedConllu, task ->
                     parseAndWriteAnnotatedConllu(annotatedConllu, task)
                }, stderrLogPath = logFilePath)
            } else {
                val handler: ((String, AnnotationWorkerPool.AnnotationTask?) -> Unit)? = if (outputFile != null) {
                    { output, _ -> writeOutput(output) }
                } else {
                    null
                }
                annotationWorkerPool = AnnotationWorkerPool(annotateWith, maxThreads, LOGGER, handler)
            }
            
            if (dockerLogMessage != null) {
                LOGGER.info(dockerLogMessage)
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
                throw ParameterException(spec.commandLine(), "--sequential is supported only with -t word2vec or -t now")
            }
        }

        // For text output formats with file output, initialize progress bar based on total zip size
        // This avoids the overhead of pre-scanning all zips to count documents
        if (outputFile != null && !quiet && (outputFormat == OutputFormat.CONLLU || 
                                              outputFormat == OutputFormat.WORD2VEC || 
                                              outputFormat == OutputFormat.NOW)) {
            val totalBytes = zipSizes.values.sum()
            if (totalBytes > 0) {
                val taskName = when (outputFormat) {
                    OutputFormat.CONLLU -> "Converting to CoNLL-U"
                    OutputFormat.WORD2VEC -> "Extracting Word2Vec"
                    OutputFormat.NOW -> "Extracting NOW format"
                    else -> "Processing"
                }
                
                // Initialize progress bar with total MB (convert bytes to MB, keep as double for precision)
                // We'll update it as each zip is completed using processedZipBytes
                val totalMB = totalBytes / (1024.0 * 1024.0)
                progressBar = ProgressBarBuilder()
                    .setTaskName(taskName)
                    .setInitialMax((totalMB * 100).toLong())  // Multiply by 100 to preserve 2 decimal places
                    .setStyle(ProgressBarStyle.COLORFUL_UNICODE_BAR)
                    .setUpdateIntervalMillis(500)
                    .showSpeed()
                    .setUnit(" MB", 100)  // Divide by 100 when displaying
                    .build()
                    
                LOGGER.info("Initialized progress tracking for ${zips.size} zip(s), total size: ${humanBytes(totalBytes)}")
            }
        }

        if (maxThreads > 1) {
            val foundry = getFoundryFromZipFileNames(zips)
            val parallelism = maxThreads.coerceAtLeast(1)
            LOGGER.info("Processing zips with ordered queue; parallelism=$parallelism; entries ${if (sequentialInZip) "sequential" else "parallel"}")
            processZipsWithQueue(zips, foundry, parallelism)
        } else {
            LOGGER.info("Processing zip files sequentially")
            Arrays.stream(zips).forEachOrdered { zipFilePath ->
                processZipFileSequentially((zipFilePath ?: "").toString(), getFoundryFromZipFileNames(zips))
            }
        }

        } finally {
        // Signal work-stealing scheduler that all foundries have been submitted
        if (workStealingSchedulerActive) {
            allFoundriesSubmitted = true
            LOGGER.info("All foundries submitted to work-stealing scheduler")
        }

        // Shutdown entry executor BEFORE closing worker pool to ensure no more tasks enqueue output after EOF
        entryExecutor?.shutdown()
        compressionExecutor?.shutdownNow()
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
            if (outputFormat == OutputFormat.KORAP_XML && morphoZipOutputStream != null) {
                try {
                    morphoZipOutputStream!!.flush()
                    morphoZipOutputStream!!.close()
                    LOGGER.info("Closed output ZIP file after annotation processing")

                    // Rename ZIP file if foundry was detected from CoNLL-U output
                    if (targetZipFileName != null && externalFoundry != null && outputFile == null) {
                        val currentFile = File(targetZipFileName!!)
                        val baseZipName = File(args[0]).name.replace(Regex("\\.zip$"), "")
                        val newFileName = File(outputDir, "$baseZipName.$externalFoundry.zip").absolutePath
                        
                        if (currentFile.absolutePath != newFileName) {
                            val newFile = File(newFileName)
                            if (currentFile.renameTo(newFile)) {
                                LOGGER.info("Renamed output ZIP from ${currentFile.name} to ${newFile.name} based on detected foundry")
                                
                                // Also rename the log file
                                var oldLogFile = File(targetZipFileName!!.replace(Regex("\\.zip$"), ".log"))
                                var newLogFile = File(newFileName.replace(Regex("\\.zip$"), ".log"))
                                
                                if (logDir != null) {
                                    oldLogFile = File(logDir, oldLogFile.name)
                                    newLogFile = File(logDir, newLogFile.name)
                                }
                                
                                if (oldLogFile.exists() && oldLogFile.renameTo(newLogFile)) {
                                    LOGGER.info("Renamed log file from ${oldLogFile.name} to ${newLogFile.name}")
                                }
                                
                                targetZipFileName = newFileName
                            } else {
                                LOGGER.warning("Failed to rename ZIP file from ${currentFile.absolutePath} to $newFileName")
                            }
                        }
                    }
                } catch (e: Exception) {
                    LOGGER.severe("ERROR closing ZIP file: ${e.message}")
                    e.printStackTrace()
                }
            }

            // Close progress bar
            progressBar?.close()

            // Check if all documents were written (only relevant for ZIP output)
            if (outputFormat == OutputFormat.KORAP_XML) {
                val sent = docsSentToAnnotation.get()
                val written = docsWrittenToZip.get()
                if (sent != written) {
                    LOGGER.warning("Document count mismatch! Sent to annotation: $sent, Written to ZIP: $written (missing: ${sent - written})")
                }
            }
        } else {
            // No external worker: ensure progress bar is closed (e.g., internal tagger -t)
            progressBar?.close()
        }
        
        // Close text output writer if it was used
        textOutputWriter?.close()
        
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
                val remainingKeys = krillData.keys.sortedWith(this::compareTextIds)
                
                // Phase 1: Submit all remaining texts for parallel compression
                if (compressionExecutor != null && !compressionExecutor!!.isShutdown) {
                    LOGGER.info("Submitting ${remainingKeys.size} remaining texts for parallel compression")
                    remainingKeys.forEach { textId ->
                        val textData = krillData[textId]
                        if (textData != null && !krillCompressedData.containsKey(textId)) {
                            compressionExecutor!!.submit {
                                compressKrillText(textId, textData)
                            }
                        }
                    }
                    
                    // Wait for compressions to complete
                    compressionExecutor!!.shutdown()
                    try {
                        var waitTime = 0L
                        while (!compressionExecutor!!.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                            waitTime += 5
                            val pending = remainingKeys.size - krillCompressedData.size
                            LOGGER.info("Compressing remaining texts... ($pending left, ${waitTime}s elapsed)")
                        }
                        LOGGER.info("All remaining texts compressed")
                    } catch (e: InterruptedException) {
                        LOGGER.warning("Interrupted while compressing remaining texts")
                        Thread.currentThread().interrupt()
                    }
                }
                
                // Phase 2: Write compressed data to TAR sequentially
                remainingKeys.forEach { textId ->
                    val textData = krillData.remove(textId) ?: return@forEach  // Skip if already removed
                    val compressedData = krillCompressedData.remove(textId)
                    
                    if (compressedData != null) {
                        // Already compressed - just write to TAR
                        val textFoundries = textData.morphoByFoundry.keys.toSet() + setOf("base")
                        val expectedForThisText = zipInventory.filter { (_, texts) -> texts.contains(textId) }.keys
                            .flatMap { zipPath ->
                                val foundry = getFoundryFromZipFileName(File(zipPath).name)
                                if (foundry.contains("-")) foundry.split("-") else listOf(foundry)
                            }
                            .toSet()

                        if (!textFoundries.containsAll(expectedForThisText)) {
                            LOGGER.warning("Outputting incomplete text $textId with foundries ${textFoundries.sorted()} (expected: ${expectedForThisText.sorted()})")
                        }
                        
                        // Write pre-compressed data directly to TAR
                        try {
                            synchronized(krillTarOutputStream!!) {
                                val tarEntry = TarArchiveEntry(compressedData.fileName)
                                tarEntry.size = compressedData.compressedBytes.size.toLong()
                                krillTarOutputStream!!.putArchiveEntry(tarEntry)
                                krillTarOutputStream!!.write(compressedData.compressedBytes)
                                krillTarOutputStream!!.closeArchiveEntry()
                            }
                            krillOutputCount.incrementAndGet()
                            incrementalProgressBar?.step()
                        } catch (e: Exception) {
                            LOGGER.severe("ERROR writing $textId to TAR: ${e.message}")
                            e.printStackTrace()
                        }
                    } else {
                        // Fallback: compress inline if not in compressed cache (shouldn't happen)
                        LOGGER.warning("Text $textId not in compressed cache, compressing inline")
                        val textFoundries = textData.morphoByFoundry.keys.toSet() + setOf("base")
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
                        incrementalProgressBar?.step()
                    }
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
    }

    private fun processZipsWithQueue(zips: Array<String>, foundry: String, parallelism: Int) {
        // For Krill output with work-stealing, use interleaved submission by text ID
        if (outputFormat == OutputFormat.KRILL && workStealingSchedulerActive) {
            processZipsInterleavedForKrill(zips)
            return
        }
        
        // Original sequential-per-ZIP processing for other formats
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
    
    /**
     * Process ZIPs for Krill output with interleaved task submission.
     * Instead of processing one ZIP/foundry at a time, submit tasks in text-ID order
     * across ALL foundries to maintain balanced progress.
     * 
     * Opens all ZIPs upfront and keeps them open to avoid repeated open/close overhead.
     */
    private fun processZipsInterleavedForKrill(zips: Array<String>) {
        // Map: foundry -> (zipFile, zipPath, entries-by-textId)
        data class FoundryData(val zipFile: ApacheZipFile, val zipPath: String, val foundry: String, val entriesByTextId: Map<String, List<ZipArchiveEntry>>)
        val foundryDataList = mutableListOf<FoundryData>()
        
        try {
            // Open all ZIPs in parallel for faster startup
            val scanParallelism = maxThreads.coerceAtLeast(1)
            val executor = java.util.concurrent.Executors.newFixedThreadPool(scanParallelism)
            
            val futures = zips.map { zipPath ->
                executor.submit<FoundryData?> {
                    val zipFoundry = getFoundryFromZipFileName(zipPath)
                    LOGGER.info("Opening ZIP: $zipPath for foundry=$zipFoundry")
                    
                    try {
                        val zipFile = openZipFile(zipPath)
                        val entries = zipFile.entries.toList()
                            .filter { !it.isDirectory && it.name.matches(Regex(".*(data|tokens|structure|morpho|dependency|sentences|constituency|header)\\.xml$")) }
                        
                        val entriesByTextId = entries.groupBy { getTextIdFromPath(it.name) }
                        
                        // Count only non-header entries for text count (header.xml is metadata, not a text)
                        val textCount = entries.count { !it.name.contains("header.xml") && (it.name.endsWith("data.xml") || it.name.endsWith("morpho.xml")) }
                        
                        // Build inventory for this ZIP (used for old flow fallback and logging)
                        // Only include actual text IDs (not corpus/doc level header IDs)
                        zipInventory[zipPath] = entries
                            .filter { !it.name.contains("header.xml") }
                            .map { getTextIdFromPath(it.name) }
                            .toMutableSet()
                        
                        // Use appropriate wording: base ZIP contains texts, annotation foundries have annotations on texts
                        if (zipFoundry == "base") {
                            LOGGER.info("  $zipPath contains $textCount texts")
                        } else {
                            LOGGER.info("  $zipPath has annotations on $textCount texts")
                        }
                        FoundryData(zipFile, zipPath, zipFoundry, entriesByTextId)
                    } catch (e: Exception) {
                        LOGGER.severe("Failed to open ZIP $zipPath: ${e.message}")
                        null
                    }
                }
            }
            
            // Collect results
            futures.forEach { future ->
                val foundryData = future.get()
                if (foundryData != null) {
                    foundryDataList.add(foundryData)
                }
            }
            
            executor.shutdown()
            
            // Get all unique text IDs across all foundries, sorted
            // Count only texts with data.xml (the actual text content)
            val allTextIds = foundryDataList
                .flatMap { foundryData ->
                    foundryData.entriesByTextId
                        .filterValues { entries -> entries.any { it.name.endsWith("data.xml") } }
                        .keys
                }
                .toSet()
                .sortedWith(this::compareTextIds)
            
            // Set expected text order for the scanner
            expectedTextOrder = allTextIds
            nextTextOrderIndex = 0
            scanOrderLogged = false
            
            LOGGER.info("Processing ${allTextIds.size} texts across ${foundryDataList.size} foundries in interleaved order")
            LOGGER.info("  Text processing order (first 20): ${expectedTextOrder.take(20)}")
            
            // Initialize progress bar now that we know the text count
            if (!quiet && allTextIds.size > 0) {
                incrementalProgressBar = ProgressBarBuilder()
                    .setTaskName(File(krillOutputFileName!!).name)
                    .setInitialMax(allTextIds.size.toLong())
                    .setStyle(ProgressBarStyle.COLORFUL_UNICODE_BAR)
                    .setUpdateIntervalMillis(500)
                    .showSpeed()
                    .build()
            }
            
            // Start incremental writer thread if we have multiple texts
            if (allTextIds.size > 1) {
                startIncrementalWriterThread()
                LOGGER.info("Enabled incremental output for ${allTextIds.size} texts")
            }
            
            // Start workers
            repeat(maxThreads) {
                entryExecutor?.execute {
                    workStealingWorker()
                }
            }
            LOGGER.info("Started $maxThreads work-stealing workers")
            
            // Build expected foundries map for each text (for completion tracking)
            allTextIds.forEach { textId ->
                val foundriesForThisText = foundryDataList
                    .filter { it.entriesByTextId.containsKey(textId) }
                    .map { it.foundry }
                    .toSet()
                expectedFoundriesPerText[textId] = foundriesForThisText
            }
            
            // CRITICAL: Process all headers FIRST to populate corpus/doc/text metadata
            // Headers must be processed before text entries so metadata is available for inheritance
            LOGGER.info("Processing headers for metadata collection...")
            foundryDataList.forEach { foundryData ->
                val allEntries = foundryData.entriesByTextId.values.flatten()
                val headerEntries = allEntries.filter { it.name.contains("header.xml") }
                
                headerEntries.forEach { headerEntry ->
                    try {
                        val headerBytes = foundryData.zipFile.getInputStream(headerEntry).readBytes()
                        val headerDoc = safeDomFactory.newDocumentBuilder().parse(ByteArrayInputStream(headerBytes))
                        val headerRoot = headerDoc.documentElement
                        headerRoot.normalize()

                        val textSigle = headerRoot.firstText("textSigle")
                        val docSigle = headerRoot.firstText("dokumentSigle")
                        val corpusSigle = headerRoot.firstText("korpusSigle")
                        val docId = textSigle?.replace('/', '_')

                        // Call appropriate metadata collection function based on what the header contains
                        if (corpusSigle != null) {
                            collectCorpusMetadata(corpusSigle, headerRoot)
                        }
                        if (docSigle != null) {
                            collectDocMetadata(docSigle, headerRoot)
                        }
                        if (docId != null) {
                            collectKrillMetadata(docId, headerRoot)
                        }
                    } catch (e: Exception) {
                        LOGGER.warning("Error processing header ${headerEntry.name}: ${e.message}")
                    }
                }
            }
            LOGGER.info("Completed header processing for metadata")
            
            // Submit tasks in text-ID order, cycling through all foundries for each text
            allTextIds.forEach { textId ->
                foundryDataList.forEach { foundryData ->
                    val textEntries = foundryData.entriesByTextId[textId]
                    if (textEntries != null && textEntries.isNotEmpty()) {
                        submitTextForFoundry(foundryData.zipFile, foundryData.zipPath, foundryData.foundry, textId, textEntries)
                    }
                }
            }
            
            LOGGER.info("Completed interleaved submission of all tasks")
            
            // Wait for all tasks to complete
            val totalTasks = foundryDataList.sumOf { it.entriesByTextId.size }
            while (foundryTaskCounts.values.sumOf { it.get() } > 0) {
                Thread.sleep(100)
            }
            
        } finally {
            // Close all ZIP files
            foundryDataList.forEach { foundryData ->
                try {
                    foundryData.zipFile.close()
                    LOGGER.info("Closed ZIP: ${foundryData.zipPath}")
                } catch (e: Exception) {
                    LOGGER.warning("Failed to close ZIP ${foundryData.zipPath}: ${e.message}")
                }
            }
        }
    }
    
    /**
     * Submit a single text's entries for a specific foundry to the work-stealing queue.
     */
    private fun submitTextForFoundry(zipFile: ApacheZipFile, zipPath: String, foundry: String, textId: String, textEntries: List<ZipArchiveEntry>) {
        val taskQueue = foundryTaskQueues.computeIfAbsent(foundry) {
            java.util.concurrent.ConcurrentLinkedQueue<PrioritizedTask>()
        }
        val taskCount = foundryTaskCounts.computeIfAbsent(foundry) { AtomicInteger(0) }
        
        // Initialize watermark on first submission
        if (!foundryWatermarks.containsKey(foundry)) {
            foundryWatermarks.putIfAbsent(foundry, textId)
        }
        
        // Task uses the already-open ZIP file
        val prioritizedTask = PrioritizedTask(foundry, textId, Runnable {
            try {
                textEntries.forEach { entry ->
                    processZipEntry(zipFile, zipPath, foundry, entry, false)
                }
                foundryWatermarks[foundry] = textId
                
                // Mark this foundry as complete for this text
                val completedFoundries = textFoundryCompletion.computeIfAbsent(textId) {
                    java.util.Collections.newSetFromMap(ConcurrentHashMap<String, Boolean>())
                }
                completedFoundries.add(foundry)
                
                // Check if all expected foundries have completed this text
                val expectedFoundries = expectedFoundriesPerText[textId] ?: emptySet()
                if (completedFoundries.containsAll(expectedFoundries)) {
                    // All foundries complete - compress immediately in this worker thread
                    // This avoids queueing overhead and ensures compression happens ASAP
                    val textData = krillData[textId]
                    if (textData != null && !krillCompressedData.containsKey(textId) && !krillCompressionFutures.containsKey(textId)) {
                        // Mark as being compressed to prevent duplicate compression
                        krillCompressionFutures[textId] = java.util.concurrent.CompletableFuture.completedFuture(null)
                        
                        // Compress inline in this worker thread (we have plenty of workers)
                        compressKrillText(textId, textData)
                        LOGGER.fine("Compressed completed text inline: $textId")
                    }
                }
            } catch (t: Throwable) {
                LOGGER.warning("Failed to process text $textId in $foundry: ${t.message}")
            } finally {
                taskCount.decrementAndGet()
            }
        })
        
        taskQueue.add(prioritizedTask)
        taskCount.incrementAndGet()
        
        // Mark foundry as having submitted tasks
        foundrySubmissionComplete[foundry] = true
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

    private fun compareTextIds(a: String, b: String): Int =
        monthAwareSortKey(a).compareTo(monthAwareSortKey(b))

    private fun openZipFile(path: String): ApacheZipFile =
        ApacheZipFile.builder()
            .setFile(File(path))
            .setCharset(StandardCharsets.UTF_8)
            .setUseUnicodeExtraFields(true)
            .get()

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
        if (outputFormat == OutputFormat.KORAP_XML && dbFactory == null) {
            // Determine output zip label. Prefer combined label if both tagger and parser are active
            var targetFoundry = "base"
            val labelParts = mutableListOf<String>()
            
            // Check if foundry override is set - if so, use it directly
            if (foundryOverride != null) {
                targetFoundry = foundryOverride!!
            } else {
                if (taggerName != null) {
                    val tagger = AnnotationToolBridgeFactory.getTagger(taggerName!!, taggerModel!!, LOGGER)
                    if (tagger != null) {
                        labelParts.add(tagger.foundry)
                    }
                }
                if (parserName != null) {
                    // Only add parser foundry if it's different from tagger foundry
                    if (taggerName == null || taggerName != parserName) {
                        labelParts.add(parserName!!)
                    }
                }
                if (labelParts.isNotEmpty()) {
                    targetFoundry = labelParts.joinToString("-")
                } else if (annotateWith.isNotEmpty()) {
                    targetFoundry = externalFoundry ?: detectFoundryFromAnnotateCmd(annotateWith)
                    LOGGER.info("Detected foundry '$targetFoundry' from annotation command: $annotateWith")
                }
            }
            dbFactory = DocumentBuilderFactory.newInstance()
            dBuilder = dbFactory!!.newDocumentBuilder()

            // Determine output filename - respect outputDir consistently
            val baseZipName = File(zipFilePath).name.replace(Regex("\\.zip$"), "")
            val autoOutputFileName = File(outputDir, "$baseZipName.$targetFoundry.zip").absolutePath
            val finalOutputFileName = outputFile ?: autoOutputFileName
            targetZipFileName = finalOutputFileName
            val outputMorphoZipFileName = finalOutputFileName
            LOGGER.info("Output ZIP file: $targetZipFileName")

            // Check for existing output file BEFORE redirecting logging, so user sees the message
            if (File(targetZipFileName).exists() && !overwrite) {
                val errorMsg = "Output file $targetZipFileName already exists. Use --force to overwrite."
                System.err.println("ERROR: $errorMsg")
                LOGGER.severe(errorMsg)
                exitProcess(1)
            }

            // Set up logging to file (like krill format does)
            var logFilePath = outputMorphoZipFileName.replace(Regex("\\.zip$"), ".log")
            
            if (logDir != null) {
                if (!logDir!!.exists()) {
                     logDir!!.mkdirs()
                }
                logFilePath = File(logDir, File(logFilePath).name).absolutePath
            }
            if (File(logFilePath).parentFile?.exists() == false) {
                 System.err.println("Error: Output directory '${File(logFilePath).parentFile}' does not exist.")
                 exitProcess(1)
            }
            val fileHandler = java.util.logging.FileHandler(logFilePath, true)
            fileHandler.formatter = ColoredFormatter()

            // Remove existing console handlers so logs only go to file
            for (logHandler in LOGGER.handlers.toList()) {
                LOGGER.removeHandler(logHandler)
            }
            
            val rootLogger = java.util.logging.Logger.getLogger("")
            for (handler in rootLogger.handlers) {
                if (handler is java.util.logging.ConsoleHandler) {
                    rootLogger.removeHandler(handler)
                }
            }
            LOGGER.addHandler(fileHandler)
            LOGGER.info("Logging redirected to: $logFilePath")

            // Mirror System.err to the same log file
            val errPs = java.io.PrintStream(java.io.BufferedOutputStream(java.io.FileOutputStream(logFilePath, true)), true)
            val oldErr = System.err
            System.setErr(errPs)

            // Delete old file if it exists
            if (File(outputMorphoZipFileName).exists()) {
                LOGGER.info("Deleting existing file: $outputMorphoZipFileName")
                File(outputMorphoZipFileName).delete()
            }

            val fileOutputStream = FileOutputStream(outputMorphoZipFileName)
            morphoZipOutputStream = ZipArchiveOutputStream(fileOutputStream).apply {
                setUseZip64(Zip64Mode.Always)
            }
            LOGGER.info("Initialized morphoZipOutputStream for $outputMorphoZipFileName")

            // Restore System.err and remove file handler on shutdown
            Runtime.getRuntime().addShutdownHook(Thread {
                try {
                    LOGGER.info("Shutting down; closing ZIP log handler")
                    LOGGER.removeHandler(fileHandler)
                    fileHandler.close()
                } catch (_: Exception) {}
                try { System.setErr(oldErr) } catch (_: Exception) {}
                try { errPs.close() } catch (_: Exception) {}
            })
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
                openZipFile(zip).use { zipFile ->
                    processZipEntriesWithPool(zipFile, zip, zipFoundry, true)
                }
            }
        } else {
            LOGGER.fine("Opening ZipFile for processing: $zipFilePath")
            try {
                // If no corresponding base ZIP exists, this IS the base ZIP
                openZipFile(zipFilePath).use { zipFile ->
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
        if (outputFormat == OutputFormat.KORAP_XML && annotationWorkerPool == null) {
            LOGGER.fine("Closing output ZIP file in processZipFile (no annotation worker pool)")
            morphoZipOutputStream!!.close()
        } else if (outputFormat == OutputFormat.KORAP_XML) {
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
                openZipFile(zip).use { zipFile ->
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
            openZipFile(zipFilePath).use { zipFile ->
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
                        "(${humanBytes(size)}). Progress: ${String.format(Locale.ROOT, "%.1f", pct)}%, " +
                        "ETA ${etaStr} at ${humanSpeed}"
            )
            
            // Update progress bar for text output formats (size-based progress in MB)
            if (!quiet && progressBar != null && 
                (outputFormat == OutputFormat.CONLLU || 
                 outputFormat == OutputFormat.WORD2VEC || 
                 outputFormat == OutputFormat.NOW)) {
                val doneMB = done / (1024.0 * 1024.0)
                progressBar?.stepTo((doneMB * 100).toLong())  // Multiply by 100 to match initialization
            }
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
        entries.sortWith(compareBy { entry ->
            // Extract text ID from path like "ZGE24/JAN/00001/base/data.xml" -> "ZGE24_JAN.00001"
            monthAwareSortKey(getTextIdFromPath(entry.name))
        })
        LOGGER.fine("Sorted entries by text ID for incremental processing")

        // Determine document count for progress: prefer data.xml, fallback to tokens.xml
        documentCount = entries.count { it.name.contains("data.xml") }
        if (documentCount == 0) {
            documentCount = entries.count { it.name.contains("tokens.xml") }
        }

        // Update total document count and start timer for external annotation or internal tagging
        // (Text output formats use size-based progress initialized upfront)
        val shouldShowProgress = (annotationWorkerPool != null || taggerName != null)
        
        if (shouldShowProgress && documentCount > 0) {
             // Only for annotation/tagging scenarios
             if (annotationStartTime.get() == 0L) {
                 val newTotal = totalDocsInInput.addAndGet(documentCount)
                annotationStartTime.set(System.currentTimeMillis())
                LOGGER.info("Starting annotation of $newTotal document(s)")
                if (!quiet) {
                     // Initialize progress bar for annotation
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
                progressBar?.maxHint(totalDocsInInput.addAndGet(documentCount).toLong())
            }
         }

        // If only one thread requested, do sequential to avoid pool overhead
        if (maxThreads <= 1) {
            entries.forEach { entry -> processZipEntry(zipFile, zipPath, foundry, entry, waitForMorpho) }
            return
        }

        // Group entries by text ID to ensure all files for a text are processed together
        val entriesByTextId = entries.groupBy { getTextIdFromPath(it.name) }
        val textIds = entriesByTextId.keys.sortedWith(this::compareTextIds)  // Process text IDs in month-aware order

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

        val latch = java.util.concurrent.CountDownLatch(textIds.size)
        
        if (workStealingSchedulerActive) {
            // Work-stealing mode for Krill output: submit tasks with throttling
            // to prevent one foundry from racing too far ahead during submission
            val taskQueue = foundryTaskQueues.computeIfAbsent(foundry) { 
                java.util.concurrent.ConcurrentLinkedQueue<PrioritizedTask>() 
            }
            val taskCount = foundryTaskCounts.computeIfAbsent(foundry) { AtomicInteger(0) }
            
            // Submission throttle: don't queue more than this many texts ahead of slowest foundry
            val submissionWindow = maxOf(100, maxThreads * 5)
            
            textIds.forEach { textId ->
                // Throttle submission if this foundry is getting too far ahead
                while (workStealingSchedulerActive && foundrySubmissionComplete.isNotEmpty()) {
                    // Find the minimum NEXT text ID across all foundries (either in queue or submitted)
                    val minNextTextId = foundryTaskQueues.entries
                        .filter { it.value.isNotEmpty() }
                        .minOfOrNull { it.value.peek()?.textId ?: "~~~~~" }
                        ?: foundryWatermarks.values.minOrNull()
                        ?: textId
                    
                    // Calculate position difference
                    val minIndex = textIds.indexOf(minNextTextId).takeIf { it >= 0 } ?: 0
                    val currentIndex = textIds.indexOf(textId).takeIf { it >= 0 } ?: 0
                    
                    // Allow submission if within window or if we're behind
                    if (currentIndex <= minIndex + submissionWindow) {
                        break
                    }
                    
                    // Wait briefly for other foundries to catch up
                    Thread.sleep(10)
                }
                
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
                        LOGGER.warning("Failed to process text $textId in $foundry: ${t.message}")
                    } finally {
                        taskCount.decrementAndGet()
                        latch.countDown()
                    }
                })
                taskQueue.add(prioritizedTask)
                taskCount.incrementAndGet()
            }
            
            // Mark this foundry as having all tasks submitted
            foundrySubmissionComplete[foundry] = true
            
            // Start workers on first foundry submission
            if (foundrySubmissionComplete.size == 1) {
                repeat(maxThreads) {
                    entryExecutor?.execute {
                        workStealingWorker()
                    }
                }
                LOGGER.info("Started $maxThreads work-stealing workers")
            }
            
            LOGGER.info("Submitted ${textIds.size} tasks for foundry $foundry to work-stealing queue")
        } else {
            // Original watermark-based throttling for non-Krill formats
            val windowSize = maxOf(1000, maxThreads * 10)
            
            textIds.forEach { textId ->
                // Check if we should throttle submission based on watermarks
                var shouldWait = true
                while (shouldWait) {
                    val minWatermark = foundryWatermarks.values.minByOrNull { it } ?: textId
                    val currentWatermark = foundryWatermarks[foundry] ?: textId
                    
                    val minIndex = textIds.indexOf(minWatermark).takeIf { it >= 0 } ?: 0
                    val currentIndex = textIds.indexOf(currentWatermark).takeIf { it >= 0 } ?: 0
                    val textIdIndex = textIds.indexOf(textId)
                    
                    if (textIdIndex < 0 || textIdIndex <= minIndex + windowSize || currentIndex <= minIndex) {
                        shouldWait = false
                    } else {
                        Thread.sleep(10)
                    }
                }
                
                val textEntries = entriesByTextId[textId] ?: emptyList()
                val prioritizedTask = PrioritizedTask(foundry, textId, Runnable {
                    try {
                        textEntries.forEach { entry ->
                            processZipEntry(zipFile, zipPath, foundry, entry, waitForMorpho)
                        }
                        foundryWatermarks[foundry] = textId
                    } catch (t: Throwable) {
                        LOGGER.warning("Failed to process text $textId: ${t.message}")
                    } finally {
                        latch.countDown()
                    }
                })
                entryExecutor?.execute(prioritizedTask)
            }
        }
        
        try {
            latch.await()
        } catch (ie: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }
    
    /**
     * Work-stealing worker: continuously picks tasks from the foundry with the lowest watermark.
     * This ensures all cores stay busy helping slower foundries catch up.
     * 
     * Strategy: Look at the NEXT text ID in each foundry's queue (peek), not the completed watermark.
     * This prevents all workers from rushing to the same "lowest" foundry.
     */
    private fun workStealingWorker() {
        while (true) {
            // Find foundry with lowest NEXT task (by peeking at queue head)
            val foundryToProcess = synchronized(foundryTaskQueues) {
                foundryTaskQueues.entries
                    .filter { entry -> entry.value.isNotEmpty() }
                    .minByOrNull { entry -> 
                        // Use the NEXT text ID in queue (peek), not the completed watermark
                        entry.value.peek()?.textId ?: "~~~~~"
                    }?.key
            }
            
            if (foundryToProcess == null) {
                // No more work available - check if we're truly done
                if (allFoundriesSubmitted) {
                    val totalRemaining = foundryTaskCounts.values.sumOf { it.get() }
                    if (totalRemaining == 0) {
                        // All work complete
                        break
                    }
                }
                // Work might still be coming or in flight, wait briefly and retry
                Thread.sleep(10)
                continue
            }
            
            // Steal a task from the chosen foundry
            val queue = foundryTaskQueues[foundryToProcess]
            val task = queue?.poll()
            
            if (task != null) {
                try {
                    task.run()
                } catch (t: Throwable) {
                    LOGGER.warning("Work-stealing worker failed on task: ${t.message}")
                }
            } else {
                // Queue was emptied by another worker, continue stealing
                Thread.yield()
            }
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
                 val isStructure = zipEntry.name.endsWith("structure.xml")
                 val needsDom = isStructure && (extractAttributesRegex.isNotEmpty() || outputFormat == OutputFormat.KRILL)
                 val isConstituency = zipEntry.name.endsWith("constituency.xml")
                 val isData = zipEntry.name.endsWith("data.xml")
                 
                 // Use DOM for data.xml (large text content) and structure/constituency (complex parsing)
                 // Use StAX for annotation files (morpho, dependency, tokens, sentences) for better performance
                 if (!needsDom && !isConstituency && !isData) {
                     processXmlEntryStax(zipFile, zipPath, zipEntry, foundry, waitForMorpho)
                     return
                 }

                LOGGER.finer("Processing entry (DOM): ${zipEntry.name}, foundry=$foundry")
                // Use thread-local DocumentBuilder (reused, much faster than creating new ones)
                val dBuilder: DocumentBuilder = threadLocalBuilder.get()
                // Reset the builder state to avoid memory leaks
                dBuilder.reset()
                
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
                        // For krill format, set tokenSource based on the foundry containing the tokens
                        if (outputFormat == OutputFormat.KRILL && foundry == "base") {
                            val textData = krillData.getOrPut(docId) { 
                                KrillJsonGenerator.KrillTextData(textId = docId) 
                            }
                            // Extract foundry from path like "CORPUS/DOC/TEXT/base/tokens.xml" -> "base"
                            val pathParts = zipEntry.name.split('/')
                            val tokensFoundry = pathParts.getOrNull(pathParts.size - 2) ?: "base"
                            textData.headerMetadata["tokenSource"] = "${tokensFoundry}#tokens"
                        }           
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
                        if (outputFormat == OutputFormat.KRILL || outputFormat == OutputFormat.CONLLU) {
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
                    outputFormat == OutputFormat.KORAP_XML && annotationWorkerPool == null -> true
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

                // Only check readiness if text hasn't been output yet
                // This prevents checking texts that were output and had their data cleared
                if ((texts[docId] != null || !textRequired) && sentences[docId] != null && tokens[docId] != null
                    && (!morphoRequired || morpho[docId] != null)
                    && (extractMetadataRegex.isEmpty() || metadata[docId] != null)
                    && !outputTexts.contains(docId)  // Skip if already output
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
                    // Collect metadata at the appropriate level(s)
                    // Note: corpus, doc, and text headers each have their own sigle fields
                    if (corpusSigle != null) {
                        collectCorpusMetadata(corpusSigle, headerRoot)
                    }
                    if (docSigle != null) {
                        collectDocMetadata(docSigle, headerRoot)
                    }
                    if (docId != null) {
                        collectKrillMetadata(docId, headerRoot)
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
                        outputFormat == OutputFormat.KORAP_XML && annotationWorkerPool == null -> true
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
                        && !outputTexts.contains(docId)  // Skip if already output
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

    private fun processXmlEntryStax(zipFile: ApacheZipFile, zipPath: String, zipEntry: ZipArchiveEntry, foundry: String, waitForMorpho: Boolean) {
        LOGGER.finer("Processing entry (StAX): ${zipEntry.name}, foundry=$foundry")
        val factory = xmlInputFactory.get()
        val inputStream = zipFile.getInputStream(zipEntry)
        val filterReader = XMLCommentFilterReader(inputStream, "UTF-8")
        val reader = try {
            factory.createXMLStreamReader(filterReader)
        } catch (e: Exception) {
            LOGGER.warning("Error creating StAX reader: " + zipEntry.name + " " + e.message)
            filterReader.close()
            return
        }

        try {
            var docId: String? = null
            while (reader.hasNext()) {
                val event = reader.next()
                if (event == XMLStreamConstants.START_ELEMENT) {
                    docId = reader.getAttributeValue(null, "docid")
                    break
                }
            }

            if (docId == null) return
            if (siglePattern != null && !Regex(siglePattern!!).containsMatchIn(docId)) return

            val fileName = zipEntry.name.replace(Regex(".*?/([^/]+\\.xml)$"), "$1")
            
            when (fileName) {
                "data.xml" -> {
                    if (!lemmaOnly) {
                        val text = extractTextStax(reader)
                        if (text != null) texts[docId] = NonBmpString(text)
                    }
                }
                "tokens.xml" -> {
                    if (!fnames.contains(docId)) fnames[docId] = zipEntry.name
                    tokens[docId] = extractSpansStax(reader, docId)
                    if (outputFormat == OutputFormat.KRILL && foundry == "base") {
                        // Set tokenSource based on the foundry containing the tokens
                        // Extract foundry from path like "CORPUS/DOC/TEXT/base/tokens.xml" -> "base"
                        val textData = krillData.getOrPut(docId) { 
                            KrillJsonGenerator.KrillTextData(textId = docId) 
                        }
                        val pathParts = zipEntry.name.split('/')
                        val tokensFoundry = pathParts.getOrNull(pathParts.size - 2) ?: "base"
                        textData.headerMetadata["tokenSource"] = "${tokensFoundry}#tokens"
                        
                        collectKrillBaseData(docId)
                    }
                }
                "morpho.xml" -> {
                    fnames[docId] = zipEntry.name
                    val (morphoSpans, allSpans) = extractMorphoSpansStax(reader)
                    
                    if (outputFormat == OutputFormat.KRILL) {
                        val morphoFoundry = getFoundryForLayer(foundry, "morpho")
                        collectKrillMorphoDataDirect(docId, morphoFoundry, morphoSpans, "morpho")
                        tokens[docId] = allSpans
                    } else {
                        val morphoMap = synchronized(morpho) {
                            morpho.getOrPut(docId) { morphoSpans }
                        }
                        if (morphoMap !== morphoSpans) {
                            synchronized(morphoMap) {
                                morphoSpans.forEach { (key, mfs) ->
                                    val existing = morphoMap[key]
                                    if (existing != null) {
                                        mfs.head = existing.head
                                        mfs.deprel = existing.deprel
                                    }
                                    morphoMap[key] = mfs
                                }
                            }
                        }
                        tokens[docId] = allSpans
                    }
                }
                "dependency.xml" -> {
                    val depMap = extractDependencySpansStax(reader)
                    if (outputFormat == OutputFormat.KRILL) {
                        val depFoundry = getFoundryForLayer(foundry, "dependency")
                        collectKrillMorphoDataDirect(docId, depFoundry, depMap, "dependency")
                    } else {
                        val morphoMap = synchronized(morpho) {
                            morpho.getOrPut(docId) { mutableMapOf() }
                        }
                        synchronized(morphoMap) {
                            depMap.forEach { (key, depSpan) ->
                                val existing = morphoMap[key]
                                if (existing != null) {
                                    existing.head = depSpan.head
                                    existing.deprel = depSpan.deprel
                                } else {
                                    morphoMap[key] = depSpan
                                }
                            }
                        }
                    }
                }
                "sentences.xml" -> {
                    if (outputFormat == OutputFormat.KRILL) {
                        val spans = extractSpansStax(reader, docId)
                        collectSentencesFromSpans(docId, foundry, spans)
                    }
                }
                "structure.xml" -> {
                    sentences[docId] = extractSentenceSpansStax(reader)
                }
            }
            
            if (outputFormat == OutputFormat.KRILL) {
                processedTextsPerZip.getOrPut(zipPath.toString()) { mutableSetOf() }.add(docId)
            }
            
            val effectiveWaitForMorpho = if (fileName == "morpho.xml") true else waitForMorpho
            
             val finalMorphoRequired = when {
                taggerName != null || parserName != null -> false
                useLemma -> true
                effectiveWaitForMorpho -> true
                outputFormat == OutputFormat.KORAP_XML && annotationWorkerPool == null -> true
                outputFormat == OutputFormat.KRILL -> false
                else -> false
            }

            val textRequired = when (outputFormat) {
                OutputFormat.WORD2VEC, OutputFormat.NOW -> !(useLemma || lemmaOnly)
                OutputFormat.KRILL -> true
                else -> true
            }

            if ((texts[docId] != null || !textRequired) && sentences[docId] != null && tokens[docId] != null
                && (!finalMorphoRequired || morpho[docId] != null)
                && (extractMetadataRegex.isEmpty() || metadata[docId] != null)
            ) {
                processText(docId, foundry)
            }

        } catch (e: Exception) {
            LOGGER.warning("Error processing StAX entry ${zipEntry.name}: ${e.message}")
            e.printStackTrace()
        } finally {
            try { reader.close() } catch (_: Exception) {}
            try { filterReader.close() } catch (_: Exception) {}
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
            formatWord2VecOutput(docId)
        } else if (outputFormat == OutputFormat.NOW) {
            formatNowOutput(docId)
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
            if (outputFormat == OutputFormat.KORAP_XML && annotationWorkerPool == null) {
                formatKorapXmlOutput(getMorphoFoundry(), docId)
            } else {
                formatConlluOutput(foundry, docId)
            }
        }

        if (annotationWorkerPool != null) {
            if (outputFormat == OutputFormat.KORAP_XML) {
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
        } else if (outputFormat != OutputFormat.KORAP_XML) {
            synchronized(System.out) {
                writeOutput(output.toString())
            }
            // Note: For text output formats, progress is now tracked by zip size in logZipProgress,
            // not by individual documents, so we don't step the progress bar here
            // Release internal char[] early
            output.setLength(0)
        } else {
            // Direct ZIP output without external annotation: write morpho.xml and, if parser is active, dependency.xml
            val morphoDir = foundryOverride ?: (taggerToolBridges[Thread.currentThread().threadId()]?.foundry ?: morphoFoundry)
            val depDir = foundryOverride ?: (parserName ?: morphoDir)
             var wroteOne = false
             // Always write morpho.xml if we have morpho annotations (tagger or from input)
             if (morpho[docId] != null && morpho[docId]!!.isNotEmpty()) {
                val context = de.ids_mannheim.korapxmltools.formatters.OutputContext(
                    docId = docId,
                    foundry = morphoDir,
                    tokens = tokens[docId],
                    sentences = sentences[docId],
                    text = texts[docId],
                    morpho = morpho[docId],
                    metadata = metadata[docId],
                    extraFeatures = extraFeatures[docId],
                    fileName = fnames[docId],
                    useLemma = useLemma,
                    extractMetadataRegex = extractMetadataRegex,
                    extractAttributesRegex = extractAttributesRegex,
                    columns = columns,
                    constituencyTrees = constituencyTrees[docId],
                    includeOffsetsInMisc = false,
                    compatibilityMode = COMPATIBILITY_MODE,
                    tokenSeparator = tokenSeparator
                )
                val morphoXml = KorapXmlFormatter.formatMorpho(context, dBuilder!!).toString()
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
                val context = de.ids_mannheim.korapxmltools.formatters.OutputContext(
                    docId = docId,
                    foundry = depDir,
                    tokens = tokens[docId],
                    sentences = sentences[docId],
                    text = texts[docId],
                    morpho = morpho[docId],
                    metadata = metadata[docId],
                    extraFeatures = extraFeatures[docId],
                    fileName = fnames[docId],
                    useLemma = useLemma,
                    extractMetadataRegex = extractMetadataRegex,
                    extractAttributesRegex = extractAttributesRegex,
                    columns = columns,
                    constituencyTrees = constituencyTrees[docId],
                    includeOffsetsInMisc = false,
                    compatibilityMode = COMPATIBILITY_MODE,
                    tokenSeparator = tokenSeparator
                )
                val depXml = KorapXmlFormatter.formatDependency(context, dBuilder!!).toString()
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
                val context = de.ids_mannheim.korapxmltools.formatters.OutputContext(
                    docId = docId,
                    foundry = constDir,
                    tokens = tokens[docId],
                    sentences = sentences[docId],
                    text = texts[docId],
                    morpho = morpho[docId],
                    metadata = metadata[docId],
                    extraFeatures = extraFeatures[docId],
                    fileName = fnames[docId],
                    useLemma = useLemma,
                    extractMetadataRegex = extractMetadataRegex,
                    extractAttributesRegex = extractAttributesRegex,
                    columns = columns,
                    constituencyTrees = constituencyTrees[docId],
                    includeOffsetsInMisc = false,
                    compatibilityMode = COMPATIBILITY_MODE,
                    tokenSeparator = tokenSeparator
                )
                val constXml = KorapXmlFormatter.formatConstituency(context, dBuilder!!).toString()
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
        if (annotationWorkerPool == null && outputFormat != OutputFormat.KORAP_XML) {
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

    private fun conlluOutput(foundry: String, docId: String): StringBuilder {
        var token_index = 0
        var real_token_index = 0
        var sentence_index = 0
        val sentencesArr = sentences[docId]
        val tokensArr = tokens[docId]
        val textVal = texts[docId]
        val constituencyComments = buildConstituencyComments(docId, tokensArr, sentencesArr, textVal)
        val output =
             StringBuilder("# foundry = $foundry\n# filename = ${fnames[docId]}\n# text_id = $docId\n").append(
                 tokenOffsetsInSentence(
                     sentences, docId, sentence_index, real_token_index, tokens
                 )
             )
        fun appendConstituencyComment(sentenceIdx: Int) {
            val comment = constituencyComments[sentenceIdx]
            if (!comment.isNullOrBlank()) {
                output.append("# constituency = ").append(comment).append("\n")
            }
        }
        appendConstituencyComment(sentence_index)
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
                appendConstituencyComment(sentence_index)
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
                    val miscWithOffset = if (annotationWorkerPool != null && outputFormat == OutputFormat.KORAP_XML) {
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
                        val miscWithOffset = if (annotationWorkerPool != null && outputFormat == OutputFormat.KORAP_XML) {
                            "Offset=${span.from}-${span.to}"
                        } else "_"
                        output.append(
                            printConlluToken(
                                token_index, tokenText, misc = miscWithOffset, columns = columns
                            )
                        )
                    }
                } else {
                    val miscWithOffset = if (annotationWorkerPool != null && outputFormat == OutputFormat.KORAP_XML) {
                        "Offset=${span.from}-${span.to}"
                    } else "_"

                    output.append(
                        printConlluToken(
                            token_index, tokenText, misc = miscWithOffset, columns = columns
                        )
                    )
                }
            } else {
                val miscWithOffset = if (annotationWorkerPool != null && outputFormat == OutputFormat.KORAP_XML) {
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

    private fun buildConstituencyComments(
        docId: String,
        tokensArr: Array<Span>?,
        sentencesArr: Array<Span>?,
        textVal: NonBmpString?
    ): Map<Int, String> {
        if (tokensArr.isNullOrEmpty() || textVal == null) return emptyMap()
        val trees = constituencyTrees[docId] ?: return emptyMap()
        if (trees.isEmpty()) return emptyMap()

        data class TokenInfo(val from: Int, val to: Int, val surface: String)

        val tokenInfos = tokensArr.map { span ->
            val safeFrom = span.from.coerceIn(0, textVal.length)
            val safeTo = span.to.coerceIn(safeFrom, textVal.length)
            val surface = if (safeFrom < safeTo) {
                textVal.substring(safeFrom, safeTo)
            } else {
                "_"
            }
            TokenInfo(safeFrom, safeTo, surface.ifBlank { "_" })
        }

        fun tokensInRange(from: Int, to: Int): List<TokenInfo> =
            tokenInfos.filter { it.from >= from && it.to <= to }

        fun escapeParens(value: String): String =
            value.replace("(", "-LRB-").replace(")", "-RRB-")

        val comments = mutableMapOf<Int, String>()

        trees.forEach { tree ->
            if (tree.nodes.isEmpty()) return@forEach

            val nodeById = tree.nodes.associateBy { it.id }
            val referencedNodeIds = tree.nodes.flatMap { node ->
                node.children.mapNotNull { child ->
                    when (child) {
                        is ConstituencyParserBridge.ConstituencyChild.NodeRef -> child.targetId
                        is ConstituencyParserBridge.ConstituencyChild.MorphoRef -> child.morphoId.takeIf { nodeById.containsKey(it) }
                    }
                }
            }.toSet()
            val rootNode = tree.nodes.firstOrNull { it.id !in referencedNodeIds } ?: tree.nodes.first()
            val visited = mutableSetOf<String>()
            val sentenceIdx = sentencesArr
                ?.indexOfFirst { rootNode.from >= it.from && rootNode.to <= it.to }
                ?.takeIf { it >= 0 }
                ?: 0
            val sentenceTokens = sentencesArr
                ?.getOrNull(sentenceIdx)
                ?.let { sentSpan -> tokensInRange(sentSpan.from, sentSpan.to) }
                ?: tokenInfos
            var sentenceTokenCursor = 0

            fun render(node: ConstituencyParserBridge.ConstituencyNode): String? {
                if (!visited.add(node.id)) return null

                val childStrings = node.children.mapNotNull { child ->
                    when (child) {
                        is ConstituencyParserBridge.ConstituencyChild.NodeRef -> {
                            val childNode = nodeById[child.targetId] ?: return@mapNotNull null
                            render(childNode)
                        }
                        is ConstituencyParserBridge.ConstituencyChild.MorphoRef -> {
                            val nextToken = sentenceTokens.getOrNull(sentenceTokenCursor++)
                                ?: return@mapNotNull null
                            val tokenText = escapeParens(nextToken.surface)
                            val label = escapeParens(nodeById[child.morphoId]?.label ?: "TOK")
                            if (label == "TOK") tokenText else "($label $tokenText)"
                        }
                    }
                }.filter { it.isNotBlank() }

                val label = escapeParens(node.label.ifBlank { "ROOT" })
                if (childStrings.isEmpty()) {
                    val fallbackTokens = tokensInRange(node.from, node.to)
                    return if (fallbackTokens.isNotEmpty()) {
                        "($label ${fallbackTokens.joinToString(" ") { escapeParens(it.surface) }})"
                    } else {
                        "($label)"
                    }
                }

                return "($label ${childStrings.joinToString(" ")})"
            }

            val rendered = render(rootNode) ?: return@forEach

            comments.merge(sentenceIdx, rendered) { old, new -> "$old | $new" }
        }

        return comments
    }

    // Formatter-based output methods using modular formatters
    private fun formatConlluOutput(foundry: String, docId: String): StringBuilder {
        // For CoNLL-U output, show foundry-specific file path instead of base tokens.xml
        val foundryFileName = if (foundry == "base") {
            fnames[docId]  // Keep base/tokens.xml for base foundry
        } else {
            fnames[docId]?.replace("/base/tokens.xml", "/$foundry/morpho.xml")
                ?: "$foundry/morpho.xml"
        }
        
        val context = de.ids_mannheim.korapxmltools.formatters.OutputContext(
            docId = docId,
            foundry = foundry,
            tokens = tokens[docId],
            sentences = sentences[docId],
            text = texts[docId],
            morpho = morpho[docId],
            metadata = metadata[docId],
            extraFeatures = extraFeatures[docId],
            fileName = foundryFileName,
            useLemma = useLemma,
            extractMetadataRegex = extractMetadataRegex,
            extractAttributesRegex = extractAttributesRegex,
            columns = columns,
            constituencyTrees = constituencyTrees[docId],
            includeOffsetsInMisc = annotationWorkerPool != null && outputFormat == OutputFormat.KORAP_XML,
            compatibilityMode = COMPATIBILITY_MODE,
            tokenSeparator = tokenSeparator
        )
        return de.ids_mannheim.korapxmltools.formatters.ConlluFormatter.format(context)
    }

    private fun formatWord2VecOutput(docId: String): StringBuilder {
        val context = de.ids_mannheim.korapxmltools.formatters.OutputContext(
            docId = docId,
            foundry = "base",
            tokens = tokens[docId],
            sentences = sentences[docId],
            text = texts[docId],
            morpho = morpho[docId],
            metadata = metadata[docId],
            extraFeatures = extraFeatures[docId],
            fileName = fnames[docId],
            useLemma = useLemma,
            extractMetadataRegex = extractMetadataRegex,
            extractAttributesRegex = extractAttributesRegex,
            columns = columns
        )
        return de.ids_mannheim.korapxmltools.formatters.Word2VecFormatter.format(context)
    }

    private fun formatNowOutput(docId: String): StringBuilder {
        val context = de.ids_mannheim.korapxmltools.formatters.OutputContext(
            docId = docId,
            foundry = "base",
            tokens = tokens[docId],
            sentences = sentences[docId],
            text = texts[docId],
            morpho = morpho[docId],
            metadata = metadata[docId],
            extraFeatures = extraFeatures[docId],
            fileName = fnames[docId],
            useLemma = useLemma,
            extractMetadataRegex = extractMetadataRegex,
            extractAttributesRegex = extractAttributesRegex,
            columns = columns
        )
        return de.ids_mannheim.korapxmltools.formatters.NowFormatter.format(context)
    }

    private fun formatKorapXmlOutput(foundry: String, docId: String): StringBuilder {
        val hasConstituencyParser = constituencyParserBridges[Thread.currentThread().threadId()] != null
        val context = de.ids_mannheim.korapxmltools.formatters.OutputContext(
            docId = docId,
            foundry = foundry,
            tokens = tokens[docId],
            sentences = sentences[docId],
            text = texts[docId],
            morpho = morpho[docId],
            metadata = metadata[docId],
            extraFeatures = extraFeatures[docId],
            fileName = fnames[docId],
            useLemma = useLemma,
            extractMetadataRegex = extractMetadataRegex,
            extractAttributesRegex = extractAttributesRegex,
            columns = columns,
            constituencyTrees = constituencyTrees[docId],
            includeOffsetsInMisc = false,
            compatibilityMode = COMPATIBILITY_MODE,
            tokenSeparator = tokenSeparator,
            documentBuilder = dBuilder,
            parserName = parserName,
            constituencyParserName = if (hasConstituencyParser) "constituency" else null
        )
        return de.ids_mannheim.korapxmltools.formatters.KorapXmlFormatter.format(context)
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

    private fun extractSpansStax(reader: XMLStreamReader, docId: String): Array<Span> {
        val list = ArrayList<Span>()
        try {
            while (reader.hasNext()) {
                val event = reader.next()
                if (event == XMLStreamConstants.START_ELEMENT && reader.localName == "span") {
                    val fromAttr = reader.getAttributeValue(null, "from")
                    val toAttr = reader.getAttributeValue(null, "to")
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
        } catch (e: Exception) {
            LOGGER.warning("Error parsing spans for $docId: ${e.message}")
        }
        return list.toTypedArray()
    }

    private fun extractTextStax(reader: XMLStreamReader): String? {
        val textBuilder = StringBuilder()
        while (reader.hasNext()) {
            val event = reader.next()
            if (event == XMLStreamConstants.CHARACTERS) {
                textBuilder.append(reader.text)
            } else if (event == XMLStreamConstants.END_ELEMENT && reader.localName == "raw_text") {
                break
            }
        }
        return if (textBuilder.isNotEmpty()) textBuilder.toString() else null
    }

    private fun extractMorphoSpansStax(reader: XMLStreamReader): Pair<MutableMap<String, MorphoSpan>, Array<Span>> {
        val UNKNOWN = Regex("(UNKNOWN|<unknown>)")
        val res: MutableMap<String, MorphoSpan> = HashMap()
        val allSpans = ArrayList<Span>()
        var currentSpan: MorphoSpan? = null
        var currentFromTo: String? = null
        var currentFName: String? = null
        val textAccumulator = StringBuilder()
        
        while (reader.hasNext()) {
            val event = reader.next()
            when (event) {
                XMLStreamConstants.START_ELEMENT -> {
                    val localName = reader.localName
                    if (localName == "span") {
                        val fromAttr = reader.getAttributeValue(null, "from")
                        val toAttr = reader.getAttributeValue(null, "to")
                        if (!fromAttr.isNullOrEmpty() && !toAttr.isNullOrEmpty()) {
                            try {
                                val from = fromAttr.toInt()
                                val to = toAttr.toInt()
                                allSpans.add(Span(from, to))
                                
                                if (reader.getAttributeValue(null, "type") != "alt") {
                                    currentSpan = MorphoSpan()
                                    currentFromTo = "$from-$to"
                                }
                            } catch (_: NumberFormatException) {}
                        }
                    } else if (localName == "f" && currentSpan != null) {
                        textAccumulator.clear()
                        currentFName = reader.getAttributeValue(null, "name")
                    } else if (localName == "symbol" && currentSpan != null && currentFName == "type") {
                        val value = reader.getAttributeValue(null, "value")?.trim()
                        if (!value.isNullOrEmpty() && currentSpan.feats == "_") {
                            currentSpan.feats = value
                        }
                    }
                }
                XMLStreamConstants.CHARACTERS -> {
                    if (currentSpan != null && currentFName != null && !reader.isWhiteSpace) {
                        textAccumulator.append(reader.text)
                    }
                }
                XMLStreamConstants.END_ELEMENT -> {
                    val localName = reader.localName
                    if (localName == "f" && currentSpan != null && currentFName != null) {
                    val value = textAccumulator.toString().trim()
                    if (value.isNotEmpty()) {
                        fun append(current: String?, new: String): String {
                            return if (current == null || current == "_") new else "$current|$new"
                        }
                        when (currentFName) {
                            "lemma" -> currentSpan.lemma = append(currentSpan.lemma, value.replace(UNKNOWN, "--"))
                            "upos" -> currentSpan.upos = append(currentSpan.upos, value)
                            "xpos", "ctag", "pos" -> currentSpan.xpos = append(currentSpan.xpos, value.replace(UNKNOWN, "--"))
                            "feats", "msd" -> currentSpan.feats = append(currentSpan.feats, value)
                            "certainty" -> currentSpan.misc = append(currentSpan.misc, value)
                        }
                    }
                    textAccumulator.clear()
                    currentFName = null
                } else if (localName == "span") {
                    if (currentSpan != null && currentFromTo != null) {
                        res[currentFromTo] = currentSpan
                    }
                    currentSpan = null
                    currentFromTo = null
                }
            }
        }
    }
    return Pair(res, allSpans.toTypedArray())
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
                    fun append(current: String?, new: String): String {
                        return if (current == null || current == "_") new else "$current|$new"
                    }
                    when (attr) {
                        "lemma" -> fs.lemma = append(fs.lemma, value.replace(UNKNOWN, "--"))
                        "upos" -> fs.upos = append(fs.upos, value)
                        "xpos", "ctag", "pos" -> fs.xpos = append(fs.xpos, value.replace(UNKNOWN, "--"))
                        "feats", "msd" -> fs.feats = append(fs.feats, value)
                        "type" -> {
                             val typeVal = feature.getElementsByTagName("symbol").item(0).attributes.getNamedItem("value").textContent.trim()
                             fs.feats = append(fs.feats, typeVal)
                        }
                        "certainty" -> fs.misc = append(fs.misc, value)
                    }
                }
            res[fromTo] = fs
        }
    return res
}

    private fun extractDependencySpansStax(reader: XMLStreamReader): MutableMap<String, MorphoSpan> {
        val res: MutableMap<String, MorphoSpan> = HashMap()
        var currentFromTo: String? = null
        var currentDeprel: String? = null
        var currentHead: String? = null
        var spanDepth = 0
        
        while (reader.hasNext()) {
            val event = reader.next()
            when (event) {
                XMLStreamConstants.START_ELEMENT -> {
                    if (reader.localName == "span") {
                        spanDepth++
                        if (spanDepth == 1) {
                            currentFromTo = "${reader.getAttributeValue(null, "from")}-${reader.getAttributeValue(null, "to")}"
                            currentDeprel = null
                            currentHead = null
                        } else if (spanDepth == 2 && currentDeprel != null) {
                            val headFrom = reader.getAttributeValue(null, "from")
                            val headTo = reader.getAttributeValue(null, "to")
                            currentHead = "$headFrom-$headTo"
                        }
                    } else if (reader.localName == "rel" && spanDepth == 1) {
                        currentDeprel = reader.getAttributeValue(null, "label")
                    }
                }
                XMLStreamConstants.END_ELEMENT -> {
                    if (reader.localName == "span") {
                        if (spanDepth == 1 && currentFromTo != null && (currentHead != null || currentDeprel != null)) {
                             res[currentFromTo] = MorphoSpan(
                                head = currentHead ?: "_",
                                deprel = currentDeprel ?: "_"
                            )
                        }
                        spanDepth--
                    }
                }
            }
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

    private fun extractSentenceSpansStax(reader: XMLStreamReader): Array<Span> {
        val list = ArrayList<Span>()
        var currentFrom: Int? = null
        var currentTo: Int? = null
        var isSentence = false
        var inF = false
        
        while (reader.hasNext()) {
            val event = reader.next()
            when (event) {
                XMLStreamConstants.START_ELEMENT -> {
                    if (reader.localName == "span") {
                        currentFrom = reader.getAttributeValue(null, "from")?.toIntOrNull()
                        currentTo = reader.getAttributeValue(null, "to")?.toIntOrNull()
                        isSentence = false
                    } else if (reader.localName == "f") {
                        inF = true
                    }
                }
                XMLStreamConstants.CHARACTERS -> {
                    if (inF && reader.text.trim() == "s") {
                        isSentence = true
                    }
                }
                XMLStreamConstants.END_ELEMENT -> {
                    if (reader.localName == "span") {
                        if (isSentence && currentFrom != null && currentTo != null) {
                            list.add(Span(currentFrom!!, currentTo!!))
                        }
                        currentFrom = null
                        currentTo = null
                        isSentence = false
                    } else if (reader.localName == "f") {
                        inF = false
                    }
                }
            }
        }
        return list.toTypedArray()
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

    internal fun parseAndWriteAnnotatedConllu(annotatedConllu: String, task: AnnotationWorkerPool.AnnotationTask?) {
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
        var extractedFoundry: String? = null

        for (line in lines) {
            when {
                line.startsWith("# foundry =") -> {
                    val foundryStr = line.substring("# foundry =".length).trim()
                    if (foundryStr.isNotEmpty()) {
                        extractedFoundry = foundryStr
                        LOGGER.fine("Extracted foundry from CoNLL-U output: $extractedFoundry")
                    }
                }
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

                    val idStr = fields[0]
                    val id = idStr.toIntOrNull()

                    val lemma = if (fields.size > 2) fields[2] else "_"
                    val upos = if (fields.size > 3) fields[3] else "_"
                    val xpos = if (fields.size > 4) fields[4] else "_"
                    val feats = if (fields.size > 5) fields[5] else "_"
                    val head = if (fields.size > 6) fields[6] else "_"
                    val deprel = if (fields.size > 7) fields[7] else "_"
                    val deps = if (fields.size > 8) fields[8] else "_"
                    val misc = if (fields.size > 9) fields[9] else "_"

                    if (id != null) {
                        val tokenIndex = id - 1
                        
                        if (currentStartOffsets != null && currentEndOffsets != null &&
                            tokenIndex >= 0 &&
                            tokenIndex < currentStartOffsets.size &&
                            tokenIndex < currentEndOffsets.size) {
        
                            val spanFrom = currentStartOffsets[tokenIndex]
                            val spanTo = currentEndOffsets[tokenIndex]
                            val spanKey = "$spanFrom-$spanTo"
        
                            morphoSpans[spanKey] = MorphoSpan(lemma, upos, xpos, feats, head, deprel, deps, misc)
                        }
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

        // Use extracted foundry from CoNLL-U output if available
        val actualFoundry = if (foundryOverride != null) {
            foundryOverride!!
        } else if (extractedFoundry != null) {
            LOGGER.info("Using foundry from CoNLL-U output: $extractedFoundry (was: $foundry)")
            // Update the global externalFoundry variable for consistent naming
            externalFoundry = extractedFoundry
            extractedFoundry
        } else {
            foundry
        }

        try {
            val context = de.ids_mannheim.korapxmltools.formatters.OutputContext(
                docId = tempDocId,
                foundry = actualFoundry,
                tokens = tokens[tempDocId],
                sentences = sentences[tempDocId],
                text = texts[tempDocId],
                morpho = morpho[tempDocId],
                metadata = metadata[tempDocId],
                extraFeatures = extraFeatures[tempDocId],
                fileName = fnames[tempDocId],
                useLemma = useLemma,
                extractMetadataRegex = extractMetadataRegex,
                extractAttributesRegex = extractAttributesRegex,
                columns = columns,
                constituencyTrees = constituencyTrees[tempDocId],
                includeOffsetsInMisc = false,
                compatibilityMode = COMPATIBILITY_MODE,
                tokenSeparator = tokenSeparator
            )
            val morphoXmlOutput = KorapXmlFormatter.formatMorpho(context, dBuilder!!)
            val fixedMorphoXml = morphoXmlOutput.toString().replace(
                "docid=\"$tempDocId\"",
                "docid=\"$docId\""
            )

            val morphoEntryPath = docId.replace(Regex("[_.]"), "/") + "/$actualFoundry/morpho.xml"

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
                val context = de.ids_mannheim.korapxmltools.formatters.OutputContext(
                    docId = tempDocId,
                    foundry = actualFoundry,
                    tokens = tokens[tempDocId],
                    sentences = sentences[tempDocId],
                    text = texts[tempDocId],
                    morpho = morpho[tempDocId],
                    metadata = metadata[tempDocId],
                    extraFeatures = extraFeatures[tempDocId],
                    fileName = fnames[tempDocId],
                    useLemma = useLemma,
                    extractMetadataRegex = extractMetadataRegex,
                    extractAttributesRegex = extractAttributesRegex,
                    columns = columns,
                    constituencyTrees = constituencyTrees[tempDocId],
                    includeOffsetsInMisc = false,
                    compatibilityMode = COMPATIBILITY_MODE,
                    tokenSeparator = tokenSeparator
                )
                val dependencyXmlOutput = KorapXmlFormatter.formatDependency(context, dBuilder!!)
                val fixedDependencyXml = dependencyXmlOutput.toString().replace(
                    "docid=\"$tempDocId\"",
                    "docid=\"$docId\""
                )

                val dependencyEntryPath = docId.replace(Regex("[_.]"), "/") + "/$actualFoundry/dependency.xml"

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

    /**
     * Convert CoNLL-U input to KorAP XML ZIP format
     * Supports:
     * - Auto-detection of foundry from "# foundry = <name>" comment
     * - Manual foundry override via -F option
     * - Multi-document input (split on "# text_id" changes)
     * - Combined foundries (e.g., "marmot-malt" → marmot/morpho.xml + malt/dependency.xml)
     * - Text ID to path conversion (WUD24_I0083.95367 → WUD24/I0083/95367)
     */
    private fun convertConlluToZip(inputStream: InputStream, outputPath: String) {
        LOGGER.info("Converting CoNLL-U to KorAP XML ZIP: $outputPath")

        // Initialize DocumentBuilder for XML generation
        if (dBuilder == null) {
            dbFactory = DocumentBuilderFactory.newInstance()
            dBuilder = dbFactory!!.newDocumentBuilder()
        }

        // Parse text_id to derive directory path: WUD24_I0083.95367 → WUD24/I0083/95367
        fun textIdToPath(textId: String): String {
            val parts = textId.split('_', limit = 2)
            if (parts.size < 2) return textId.replace('.', '/')
            val corpus = parts[0]
            val remainder = parts[1].replace('.', '/')
            return "$corpus/$remainder"
        }

        // Read all input and split into documents
        data class ConlluDocument(
            val textId: String,
            val foundry: String,
            val lines: List<String>
        )

        val documents = mutableListOf<ConlluDocument>()
        val reader = BufferedReader(InputStreamReader(inputStream, StandardCharsets.UTF_8))
        var currentTextId: String? = null
        var currentFoundry: String? = null
        var currentLines = mutableListOf<String>()

        reader.forEachLine { line ->
            when {
                line.startsWith("# text_id = ") -> {
                    // Save previous document if exists
                    if (currentTextId != null && currentFoundry != null && currentLines.isNotEmpty()) {
                        documents.add(ConlluDocument(currentTextId!!, currentFoundry!!, currentLines.toList()))
                        currentLines = mutableListOf()
                    }
                    currentTextId = line.substring("# text_id = ".length).trim()
                }
                line.startsWith("# foundry = ") -> {
                    val detectedFoundry = line.substring("# foundry = ".length).trim()
                    currentFoundry = foundryOverride ?: detectedFoundry
                }
                else -> {
                    currentLines.add(line)
                }
            }
        }

        // Add final document
        if (currentTextId != null && currentFoundry != null && currentLines.isNotEmpty()) {
            documents.add(ConlluDocument(currentTextId!!, currentFoundry!!, currentLines.toList()))
        }

        if (documents.isEmpty()) {
            LOGGER.severe("No documents found in CoNLL-U input (missing '# text_id' and '# foundry' comments)")
            throw IllegalArgumentException("Invalid CoNLL-U format: missing required comments '# text_id' and '# foundry'")
        }

        LOGGER.info("Found ${documents.size} document(s) in CoNLL-U input")

        // Create output ZIP
        val outputFile = File(outputPath)
        if (outputFile.exists() && !overwrite) {
            LOGGER.severe("Output file already exists: $outputPath (use -f to overwrite)")
            throw IOException("Output file already exists: $outputPath")
        }

        val zipOutputStream = ZipArchiveOutputStream(BufferedOutputStream(FileOutputStream(outputFile)))
        zipOutputStream.setUseZip64(Zip64Mode.AsNeeded)

        try {
            // Process each document
            documents.forEach { doc ->
                LOGGER.fine("Processing document: ${doc.textId}, foundry: ${doc.foundry}")

                // Parse CoNLL-U content
                val morphoSpans = mutableMapOf<String, MorphoSpan>()
                var currentStartOffsets: List<Int>? = null
                var currentEndOffsets: List<Int>? = null
                var tokenIndexInSentence = 0
                val sentenceSpans = mutableListOf<Span>()
                var sentenceStartOffset: Int? = null
                var sentenceEndOffset: Int? = null

                for (line in doc.lines) {
                    when {
                        line.startsWith("# start_offsets =") -> {
                            val offsetsStr = line.substring("# start_offsets =".length).trim()
                            val allOffsets = offsetsStr.split(Regex("\\s+")).mapNotNull { it.toIntOrNull() }
                            if (allOffsets.isEmpty()) {
                                LOGGER.severe("Missing start_offsets for text ${doc.textId}")
                                throw IllegalArgumentException("CoNLL-U format error: missing start_offsets for text ${doc.textId}")
                            }
                            sentenceStartOffset = allOffsets.firstOrNull()
                            currentStartOffsets = if (allOffsets.size > 1) allOffsets.drop(1) else allOffsets
                            tokenIndexInSentence = 0
                        }
                        line.startsWith("# end_offsets =") -> {
                            val offsetsStr = line.substring("# end_offsets =".length).trim()
                            val allOffsets = offsetsStr.split(Regex("\\s+")).mapNotNull { it.toIntOrNull() }
                            if (allOffsets.isEmpty()) {
                                LOGGER.severe("Missing end_offsets for text ${doc.textId}")
                                throw IllegalArgumentException("CoNLL-U format error: missing end_offsets for text ${doc.textId}")
                            }
                            sentenceEndOffset = allOffsets.firstOrNull()
                            currentEndOffsets = if (allOffsets.size > 1) allOffsets.drop(1) else emptyList()
                        }
                        line.isEmpty() -> {
                            // Sentence boundary
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

                            val idStr = fields[0]
                            val rangeMatch = Regex("^([0-9]+)-([0-9]+)$").find(idStr)
                            
                            // If it's a range (e.g. 1-2), we might want to skip it if it's just surface form 
                            // coverage for following tokens, OR process it if we want to annotate the MWT.
                            // However, the internal logic expects 1-to-1 mapping with offsets list for tokens.
                            // For now, only process single integer IDs.
                            // (If we support MWT annotations, we'd need to map range to start of first and end of last coverage?)
                            
                            val id = idStr.toIntOrNull()
                            
                            if (id != null) {
                                // CoNLL-U IDs are 1-based, offsets lists are 0-based relative to tokens
                                val tokenIndex = id - 1
                                
                                val lemma = if (fields.size > 2) fields[2] else "_"
                                val upos = if (fields.size > 3) fields[3] else "_"
                                val xpos = if (fields.size > 4) fields[4] else "_"
                                val feats = if (fields.size > 5) fields[5] else "_"
                                val head = if (fields.size > 6) fields[6] else "_"
                                val deprel = if (fields.size > 7) fields[7] else "_"
                                val deps = if (fields.size > 8) fields[8] else "_"
                                val misc = if (fields.size > 9) fields[9] else "_"

                                if (currentStartOffsets == null || currentEndOffsets == null) {
                                    LOGGER.severe("Token found before offset comments in text ${doc.textId}")
                                    throw IllegalArgumentException("CoNLL-U format error: tokens found before offset comments in text ${doc.textId}")
                                }

                                if (tokenIndex >= 0 && 
                                    tokenIndex < currentStartOffsets.size &&
                                    tokenIndex < currentEndOffsets.size) {

                                    val spanFrom = currentStartOffsets[tokenIndex]
                                    val spanTo = currentEndOffsets[tokenIndex]
                                    val spanKey = "$spanFrom-$spanTo"

                                    morphoSpans[spanKey] = MorphoSpan(lemma, upos, xpos, feats, head, deprel, deps, misc)
                                }
                            }
                        }
                    }
                }

                // Capture final sentence if not ended with empty line
                if (sentenceStartOffset != null && sentenceEndOffset != null) {
                    sentenceSpans.add(Span(sentenceStartOffset!!, sentenceEndOffset!!))
                }

                if (morphoSpans.isEmpty()) {
                    LOGGER.warning("No morpho spans found for text ${doc.textId}, skipping")
                    return@forEach
                }

                // Determine which layers to generate based on foundry and content
                val hasDependencies = morphoSpans.values.any { span ->
                    span.head != null && span.head != "_" && span.deprel != null && span.deprel != "_"
                }

                // Get foundry names for each layer (handles combined foundries like "marmot-malt")
                val morphoFoundry = getFoundryForLayer(doc.foundry, "morpho")
                val dependencyFoundry = if (hasDependencies) getFoundryForLayer(doc.foundry, "dependency") else null

                // Store data in temp maps for XML generation
                val tempDocId = "_temp_conllu_${doc.textId}"
                morpho[tempDocId] = morphoSpans
                if (sentenceSpans.isNotEmpty()) {
                    sentences[tempDocId] = sentenceSpans.toTypedArray()
                } else if (morphoSpans.isNotEmpty()) {
                    // Fallback: create single sentence spanning all tokens
                    val minOffset = morphoSpans.keys.minOfOrNull { it.split("-")[0].toInt() } ?: 0
                    val maxOffset = morphoSpans.keys.maxOfOrNull { it.split("-")[1].toInt() } ?: 0
                    sentences[tempDocId] = arrayOf(Span(minOffset, maxOffset))
                }

                // Generate morpho.xml
                try {
                    val basePath = textIdToPath(doc.textId)
                    val morphoPath = "$basePath/$morphoFoundry/morpho.xml"

                    val context = de.ids_mannheim.korapxmltools.formatters.OutputContext(
                        docId = tempDocId,
                        foundry = morphoFoundry,
                        tokens = getTokenSpansFromMorho(morphoSpans),
                        sentences = sentences[tempDocId],
                        text = null,
                        morpho = morpho[tempDocId],
                        metadata = null,
                        extraFeatures = null,
                        fileName = null,
                        useLemma = useLemma,
                        extractMetadataRegex = extractMetadataRegex,
                        extractAttributesRegex = extractAttributesRegex,
                        columns = columns,
                        constituencyTrees = null,
                        includeOffsetsInMisc = false,
                        compatibilityMode = COMPATIBILITY_MODE,
                        tokenSeparator = tokenSeparator
                    )

                    val morphoXmlOutput = KorapXmlFormatter.formatMorpho(context, dBuilder!!)
                    val fixedMorphoXml = morphoXmlOutput.toString().replace(
                        "docid=\"$tempDocId\"",
                        "docid=\"${doc.textId}\""
                    )

                    val morphoZipEntry = ZipArchiveEntry(morphoPath)
                    morphoZipEntry.unixMode = ZIP_ENTRY_UNIX_MODE
                    zipOutputStream.putArchiveEntry(morphoZipEntry)
                    zipOutputStream.write(fixedMorphoXml.toByteArray())
                    zipOutputStream.closeArchiveEntry()

                    LOGGER.fine("Wrote $morphoPath (${fixedMorphoXml.length} bytes)")
                } catch (e: Exception) {
                    LOGGER.severe("ERROR generating morpho.xml for ${doc.textId}: ${e.message}")
                    throw e
                }

                // Generate dependency.xml if dependencies present
                if (hasDependencies && dependencyFoundry != null) {
                    try {
                        val basePath = textIdToPath(doc.textId)
                        val dependencyPath = "$basePath/$dependencyFoundry/dependency.xml"

                        val context = de.ids_mannheim.korapxmltools.formatters.OutputContext(
                            docId = tempDocId,
                            foundry = dependencyFoundry,
                            tokens = getTokenSpansFromMorho(morphoSpans),
                            sentences = sentences[tempDocId],
                            text = null,
                            morpho = morpho[tempDocId],
                            metadata = null,
                            extraFeatures = null,
                            fileName = null,
                            useLemma = useLemma,
                            extractMetadataRegex = extractMetadataRegex,
                            extractAttributesRegex = extractAttributesRegex,
                            columns = columns,
                            constituencyTrees = null,
                            includeOffsetsInMisc = false,
                            compatibilityMode = COMPATIBILITY_MODE,
                            tokenSeparator = tokenSeparator
                        )

                        val dependencyXmlOutput = KorapXmlFormatter.formatDependency(context, dBuilder!!)
                        val fixedDependencyXml = dependencyXmlOutput.toString().replace(
                            "docid=\"$tempDocId\"",
                            "docid=\"${doc.textId}\""
                        )

                        val dependencyZipEntry = ZipArchiveEntry(dependencyPath)
                        dependencyZipEntry.unixMode = ZIP_ENTRY_UNIX_MODE
                        zipOutputStream.putArchiveEntry(dependencyZipEntry)
                        zipOutputStream.write(fixedDependencyXml.toByteArray())
                        zipOutputStream.closeArchiveEntry()

                        LOGGER.fine("Wrote $dependencyPath (${fixedDependencyXml.length} bytes)")
                    } catch (e: Exception) {
                        LOGGER.severe("ERROR generating dependency.xml for ${doc.textId}: ${e.message}")
                        throw e
                    }
                }

                // Cleanup temp data
                morpho.remove(tempDocId)
                sentences.remove(tempDocId)
            }

            LOGGER.info("Successfully wrote ${documents.size} document(s) to $outputPath")
        } finally {
            zipOutputStream.close()
        }
    }

    // Collect structural spans from structure.xml for krill format
    private fun collectKrillStructureSpans(docId: String, spans: NodeList) {
        // Skip if already output (thread-safe check with ConcurrentHashMap.KeySet)
        if (outputTexts.contains(docId)) return

        val textData = krillData.getOrPut(docId) {
            KrillJsonGenerator.KrillTextData(textId = docId)
        }

        synchronized(textData) {
            // Only clear dereko structure spans when re-collecting (preserve external foundry spans)
            // This allows multiple ZIPs to be processed without losing annotation layer structure
            val nonDerekoSpans = textData.structureSpans.filter { !it.layer.startsWith("dereko/") }
            if (textData.structureSpans.size > nonDerekoSpans.size) {
                LOGGER.fine("Clearing ${textData.structureSpans.size - nonDerekoSpans.size} dereko structure spans for $docId before re-collecting")
                textData.structureSpans.clear()
                textData.structureSpans.addAll(nonDerekoSpans)
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
                    textData.structureSpans.add(KrillJsonGenerator.StructureSpan(
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
        val children: MutableList<ConstituencyParserBridge.ConstituencyChild> = mutableListOf()
    )

    private fun collectSentences(docId: String, foundry: String, spans: NodeList) {
        if (outputTexts.contains(docId)) return

        val textData = krillData.getOrPut(docId) {
            KrillJsonGenerator.KrillTextData(textId = docId)
        }

        synchronized(textData) {
            if (textData.sentencesCollectedByFoundry.contains(foundry)) return
            for (i in 0 until spans.length) {
                val span = spans.item(i) as? Element ?: continue
                val from = span.getAttribute("from").toIntOrNull() ?: continue
                val to = span.getAttribute("to").toIntOrNull() ?: continue
                textData.structureSpans.add(
                    KrillJsonGenerator.StructureSpan(
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

    private fun collectSentencesFromSpans(docId: String, foundry: String, spans: Array<Span>) {
        if (outputTexts.contains(docId)) return
        val textData = krillData.getOrPut(docId) { KrillJsonGenerator.KrillTextData(textId = docId) }
        synchronized(textData) {
            if (textData.sentencesCollectedByFoundry.contains(foundry)) return
            for (span in spans) {
                textData.structureSpans.add(
                    KrillJsonGenerator.StructureSpan(
                        layer = "$foundry/s:s",
                        from = span.from,
                        to = span.to,
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
                    node.children.add(ConstituencyParserBridge.ConstituencyChild.NodeRef(target))
                    nonRootIds.add(target)
                } else {
                    val uri = rel.getAttribute("uri")
                    if (!uri.isNullOrBlank()) {
                        val normalized = uri.removePrefix("morpho.xml#")
                        if (normalized.isNotBlank()) {
                            node.children.add(ConstituencyParserBridge.ConstituencyChild.MorphoRef(normalized))
                            nonRootIds.add(normalized)
                        }
                    }
                }
            }
            nodesById[id] = node
        }

        if (nodesById.isEmpty()) return

        val textData = krillData.getOrPut(docId) {
            KrillJsonGenerator.KrillTextData(textId = docId)
        }

        synchronized(textData) {
            if (textData.constituencyCollectedByFoundry.contains(foundry)) return
            LOGGER.fine("Collecting constituency for $docId from foundry $foundry: ${nodesById.size} nodes, roots=${nodesById.keys.count { it !in nonRootIds }}")

            fun traverse(nodeId: String, depth: Int) {
                val node = nodesById[nodeId] ?: return
                textData.structureSpans.add(
                    KrillJsonGenerator.StructureSpan(
                        layer = "$foundry/c:${node.label}",
                        from = node.from,
                        to = node.to,
                        tokenFrom = -1,
                        tokenTo = -1,
                        depth = depth,
                        attributes = emptyMap()
                    )
                )
                node.children.forEach { child ->
                    if (child is ConstituencyParserBridge.ConstituencyChild.NodeRef) {
                        traverse(child.targetId, depth + 1)
                    }
                }
            }

            val rootIds = nodesById.keys.filter { it !in nonRootIds }
            rootIds.forEach { traverse(it, 0) }
            textData.constituencyCollectedByFoundry.add(foundry)
        }

        // Also cache constituency trees for downstream outputs (e.g., CoNLL-U comments)
        if (nodesById.isNotEmpty()) {
            val nodeRefTargets = nodesById.values
                .flatMap { node -> node.children.mapNotNull { child -> (child as? ConstituencyParserBridge.ConstituencyChild.NodeRef)?.targetId } }
                .toMutableSet()
            nodesById.values.forEach { node ->
                node.children.forEach { child ->
                    if (child is ConstituencyParserBridge.ConstituencyChild.MorphoRef && nodesById.containsKey(child.morphoId)) {
                        nodeRefTargets.add(child.morphoId)
                    }
                }
            }
            val rootIds = nodesById.keys.filter { it !in nodeRefTargets }

            val trees = mutableListOf<ConstituencyParserBridge.ConstituencyTree>()

            fun copySubtree(nodeId: String, visited: MutableSet<String>): ConstituencyParserBridge.ConstituencyNode? {
                if (!visited.add(nodeId)) return null
                val source = nodesById[nodeId] ?: return null
                val copiedChildren = mutableListOf<ConstituencyParserBridge.ConstituencyChild>()
                source.children.forEach { child ->
                    when (child) {
                        is ConstituencyParserBridge.ConstituencyChild.NodeRef -> copiedChildren.add(child)
                        is ConstituencyParserBridge.ConstituencyChild.MorphoRef -> copiedChildren.add(child)
                    }
                }
                return ConstituencyParserBridge.ConstituencyNode(
                    id = source.id,
                    label = source.label,
                    from = source.from,
                    to = source.to,
                    children = copiedChildren
                )
            }

            rootIds.forEachIndexed { idx, rootId ->
                val visited = mutableSetOf<String>()
                val collectedNodes = mutableListOf<ConstituencyParserBridge.ConstituencyNode>()

                fun collect(nodeId: String) {
                    val node = copySubtree(nodeId, visited) ?: return
                    collectedNodes.add(node)
                    node.children.forEach { child ->
                        if (child is ConstituencyParserBridge.ConstituencyChild.NodeRef) {
                            collect(child.targetId)
                        }
                    }
                }

                collect(rootId)

                if (collectedNodes.isNotEmpty()) {
                    val sentenceId = rootId.substringBefore("_n").takeIf { it.isNotBlank() } ?: "s${idx + 1}"
                    trees.add(ConstituencyParserBridge.ConstituencyTree(sentenceId = sentenceId, nodes = collectedNodes))
                }
            }

            if (trees.isNotEmpty()) {
                constituencyTrees.compute(docId) { _, existing ->
                    existing?.takeIf { it.isNotEmpty() } ?: trees
                }
            }
        }
    }

    // Collect rich metadata from header.xml for krill format
    private fun collectKrillMetadata(docId: String, headerRoot: Element) {
        if (outputTexts.contains(docId)) return

        val textData = krillData.getOrPut(docId) { KrillJsonGenerator.KrillTextData(textId = docId) }

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

            // Extract textExternalLinks from biblNote[@n='url']
            val biblNoteUrl = analytic.firstElement("biblNote") { it.getAttribute("n") == "url" }
                ?.textContent?.trim()?.takeIf { it.isNotEmpty() }
                ?: monogr.firstElement("biblNote") { it.getAttribute("n") == "url" }
                    ?.textContent?.trim()?.takeIf { it.isNotEmpty() }
            metadata.putIfNotBlank("textExternalLinks", biblNoteUrl)

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
            metadata.putIfNotBlank("pubPlace", headerRoot.firstText("pubPlace"))
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
            KrillJsonGenerator.KrillTextData(textId = docId)
        }

        synchronized(textData) {
            // Capture values locally to avoid TOCTOU race conditions
            // If text content is missing but we have tokens, try to reconstruct it or warn
            if (textData.textContent == null) {
                LOGGER.warning("Text content missing for $docId, but tokens present. Krill output may be incomplete.")
                textData.textContent = de.ids_mannheim.korapxmltools.NonBmpString("")
            }
            val text = texts[docId]
            if (text != null) {
                textData.textContent = text
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
            KrillJsonGenerator.KrillTextData(textId = docId)
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
            KrillJsonGenerator.KrillTextData(textId = docId)
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
            KrillJsonGenerator.KrillTextData(textId = docId)
        }
        if (!wasNew) {
            LOGGER.info("  Found existing KrillTextData for $docId, foundries=${textData.morphoByFoundry.keys}")
        }

        // Collect text content (only from base foundry)
        if (foundry == "base" && texts[docId] != null) {
            synchronized(textData) {
                textData.textContent = texts[docId]!!
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
        
        // Start compression thread pool (parallel compression)
        val compressionThreads = maxThreads.coerceAtLeast(2)
        compressionExecutor = java.util.concurrent.Executors.newFixedThreadPool(compressionThreads) { r ->
            Thread(r, "KrillCompressor-${Thread.currentThread().threadId()}")
        }
        LOGGER.info("Started compression thread pool with $compressionThreads threads")
        
        // Start single writer thread (sequential TAR writing)
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

    // Compress text data in parallel, then write to TAR sequentially
    private fun compressKrillText(textId: String, textData: KrillJsonGenerator.KrillTextData) {
        try {
            val json = synchronized(textData) {
                // Synchronize access to textData to prevent ConcurrentModificationException
                // during JSON generation while other threads might still be modifying it
                val corpusSigle = textId.substringBefore('_')
                val docSigle = textId.substringBeforeLast('.')

                // Apply corpus-level metadata (only if not already set with a non-empty value)
                corpusMetadata[corpusSigle]?.forEach { (key, value) ->
                    val currentValue = textData.headerMetadata[key]
                    // Inherit if: key doesn't exist, OR current value is empty/blank
                    val shouldInherit = when (currentValue) {
                        null -> true
                        is String -> currentValue.isBlank()
                        else -> false
                    }
                    if (shouldInherit && value != null) {
                        // Only set non-empty values
                        when (value) {
                            is String -> if (value.isNotBlank()) textData.headerMetadata[key] = value
                            is List<*> -> if (value.isNotEmpty()) textData.headerMetadata[key] = value
                            is Map<*, *> -> if (value.isNotEmpty()) textData.headerMetadata[key] = value
                            else -> textData.headerMetadata[key] = value
                        }
                    }
                }

                // Apply doc-level metadata (only if not already set with a non-empty value)
                docMetadata[docSigle]?.forEach { (key, value) ->
                    val currentValue = textData.headerMetadata[key]
                    // Inherit if: key doesn't exist, OR current value is empty/blank
                    val shouldInherit = when (currentValue) {
                        null -> true
                        is String -> currentValue.isBlank()
                        else -> false
                    }
                    if (shouldInherit) {
                        // Only set non-empty values
                        when (value) {
                            is String -> if (value.isNotBlank()) textData.headerMetadata[key] = value
                            is List<*> -> if (value.isNotEmpty()) textData.headerMetadata[key] = value
                            is Map<*, *> -> if (value.isNotEmpty()) textData.headerMetadata[key] = value
                            else -> textData.headerMetadata[key] = value
                        }
                    }
                }

                KrillJsonGenerator.generate(textData, corpusMetadata, docMetadata, includeNonWordTokens)
            }
            
            // Choose compression format based on --lz4 flag
            val (jsonFileName, compressedData) = if (useLz4) {
                val fileName = textId.replace("_", "-").replace(".", "-") + ".json.lz4"
                val jsonBytes = json.toByteArray(Charsets.UTF_8)
                val byteOut = ByteArrayOutputStream()
                net.jpountz.lz4.LZ4FrameOutputStream(byteOut).use { lz4Out ->
                    lz4Out.write(jsonBytes)
                }
                Pair(fileName, byteOut.toByteArray())
            } else {
                // Use GZIP with level 1 compression for speed
                val fileName = textId.replace("_", "-").replace(".", "-") + ".json.gz"
                val jsonBytes = json.toByteArray(Charsets.UTF_8)
                val byteOut = ByteArrayOutputStream(jsonBytes.size)
                
                // Create GZIPOutputStream with level 1 (fast) compression
                val gzipOut = object : java.util.zip.GZIPOutputStream(byteOut) {
                    init {
                        def.setLevel(1)
                    }
                }
                gzipOut.use { it.write(jsonBytes) }
                
                Pair(fileName, byteOut.toByteArray())
            }

            // Store compressed data for sequential TAR writing
            krillCompressedData[textId] = CompressedKrillData(textId, jsonFileName, compressedData)
            LOGGER.finer("Compressed text $textId (${compressedData.size} bytes)")
        } catch (e: Exception) {
            LOGGER.severe("ERROR compressing $textId: ${e.message}")
            e.printStackTrace()
        }
    }

    // Scan all texts and output any that are complete
    private fun scanAndOutputCompleteTexts(forceScan: Boolean = false) {
        if ((shutdownIncrementalWriter && !forceScan) || !tarStreamOpen) return

        if (expectedTextOrder.isEmpty()) return

        // Phase 1: Find ALL ready texts (order doesn't matter for output)
        val readyTexts = mutableListOf<String>()
        var checkedCount = 0
        var newFlowCount = 0
        var oldFlowCount = 0
        
        for (textId in krillData.keys) {
            // Skip if already output
            if (outputTexts.contains(textId)) continue
            
            checkedCount++
            
            // Check completion using the new work-stealing tracking if available,
            // otherwise fall back to the old ZIP-based tracking
            val allProcessed = if (expectedFoundriesPerText.containsKey(textId)) {
                // New flow: check if all expected foundries have completed this text
                newFlowCount++
                val completedFoundries = textFoundryCompletion[textId] ?: emptySet()
                val expectedFoundries = expectedFoundriesPerText[textId] ?: emptySet()
                completedFoundries.containsAll(expectedFoundries)
            } else {
                // Old flow: check if all relevant ZIPs have processed this text
                oldFlowCount++
                val relevantZips = zipInventory.filter { (_, texts) -> texts.contains(textId) }.keys
                relevantZips.all { path ->
                    processedTextsPerZip[path]?.contains(textId) == true
                }
            }
            
            if (allProcessed) {
                readyTexts.add(textId)
                
                // Start compression immediately if not already started
                if (!krillCompressedData.containsKey(textId) && !krillCompressionFutures.containsKey(textId)) {
                    val textData = krillData[textId]
                    if (textData != null) {
                        val future = compressionExecutor?.submit {
                            compressKrillText(textId, textData)
                        }
                        if (future != null) {
                            krillCompressionFutures[textId] = future
                        }
                    }
                }
            }
        }

        // Phase 2: Write compressed texts to TAR (only those already compressed, no waiting)
        // Event-driven: if compression is done, write it; otherwise skip and try next scan
        var outputCount = 0
        var waitingForCompression = 0
        for (textId in readyTexts) {
            // Check if already compressed (event-based: poll, don't wait)
            val compressedData = krillCompressedData[textId]
            
            // If not yet compressed, skip for now (will be picked up in next scan)
            if (compressedData == null) {
                waitingForCompression++
                // Check if future is done without blocking
                val future = krillCompressionFutures[textId]
                if (future?.isDone == true) {
                    // Compression just finished but data not yet in map - race condition, will catch next scan
                    LOGGER.finest("Compression finished for $textId but data not yet stored, retrying next scan")
                }
                continue
            }
            
            // Remove future since compression is complete
            krillCompressionFutures.remove(textId)
            
            if (outputTexts.add(textId)) {
                val relevantZips = zipInventory.filter { (_, texts) -> texts.contains(textId) }.keys
                tarStreamLock.lock()
                try {
                    if (tarStreamOpen) {
                        try {
                            // Write to TAR
                            synchronized(krillTarOutputStream!!) {
                                val tarEntry = TarArchiveEntry(compressedData.fileName)
                                tarEntry.size = compressedData.compressedBytes.size.toLong()
                                krillTarOutputStream!!.putArchiveEntry(tarEntry)
                                krillTarOutputStream!!.write(compressedData.compressedBytes)
                                krillTarOutputStream!!.closeArchiveEntry()
                            }
                            
                            val count = krillOutputCount.incrementAndGet()
                            incrementalProgressBar?.step()
                            outputCount++
                            LOGGER.fine("Output text $textId (processed by ${relevantZips.size} ZIPs, ${krillData.size} still pending)")
                        } catch (e: IOException) {
                            LOGGER.warning("Cannot output text $textId: stream closed")
                            tarStreamOpen = false
                            break
                        }
                    }
                } finally {
                    tarStreamLock.unlock()
                }
            }

            // Clean up all data structures
            krillData.remove(textId)
            krillCompressedData.remove(textId)
            krillCompressionFutures.remove(textId)

            // Clean up tracking data for this text
            val relevantZips = zipInventory.filter { (_, texts) -> texts.contains(textId) }.keys
            relevantZips.forEach { path ->
                zipInventory[path]?.remove(textId)
                processedTextsPerZip[path]?.remove(textId)
            }
        }

        if (outputCount > 0 || readyTexts.isNotEmpty()) {
            LOGGER.fine("Scan: checked=$checkedCount, newFlow=$newFlowCount, oldFlow=$oldFlowCount, ready=${readyTexts.size}, compressed=$outputCount, waitingCompression=$waitingForCompression, pending=${krillData.size}")
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
            
            // Note: Don't shutdown compressionExecutor here - let the main finalization code
            // handle remaining texts with parallel compression, then shut it down there
            
            LOGGER.info("Final scan completed, output ${remainingBeforeScan - krillData.size} texts")

            incrementalOutputScheduler = null
            // Don't null compressionExecutor - it will be used for final compression
        }
    }

    // Build per-ZIP inventory of which texts each ZIP contains
    // This allows us to know when a text has been processed by all ZIPs that should contain it
    private fun buildZipInventory(zipPaths: Array<String>) {
        LOGGER.info("Building per-ZIP inventory to track text completeness...")
        zipInventory.clear()

        // Scan ZIPs in parallel for faster startup
        val scanParallelism = maxThreads.coerceAtLeast(1)
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
                        openZipFile(zipPath).use { zipFile ->
                            val entries = zipFile.entries
                            
                            // For base ZIPs: only count data.xml (the actual text content)
                            // For annotation foundries: count their annotation files
                            val pattern = if (isAnnotationFoundry) {
                                Regex(".*/(?:morpho|dependency|constituency)\\.xml$")
                            } else {
                                Regex(".*/data\\.xml$")
                            }
                            
                            // Extract docId from path instead of parsing XML
                            // Base ZIP path: CORPUS/DOC/TEXT/data.xml
                            // Annotation ZIP path: CORPUS/DOC/TEXT/foundry/dependency.xml
                            val pathPattern = if (isAnnotationFoundry) {
                                Regex("([^/]+)/([^/]+)/([^/]+)/[^/]+/(?:morpho|dependency|constituency)\\.xml$")
                            } else {
                                Regex("([^/]+)/([^/]+)/([^/]+)/data\\.xml$")
                            }
                            
                            while (entries.hasMoreElements()) {
                                val entry = entries.nextElement()
                                
                                if (entry.name.matches(pattern)) {
                                    val matchResult = pathPattern.find(entry.name)
                                    if (matchResult != null) {
                                        val (corpus, doc, text) = matchResult.destructured
                                        val docId = "${corpus}_${doc}.${text}"
                                        textsInThisZip.add(docId)
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

        LOGGER.info("ZIP inventory built: ${zipPaths.size} ZIPs scanned")
        // Calculate total unique texts
        val allTexts = zipInventory.values.flatten().toSet()
        expectedTextOrder = allTexts.sortedWith(this::compareTextIds)
        nextTextOrderIndex = 0
        scanOrderLogged = false
        LOGGER.info("  Total unique texts across all ZIPs: ${allTexts.size}")
        LOGGER.info("  Text processing order (first 20): ${expectedTextOrder.take(20)}")
    }

    // Output a single text to Krill TAR (thread-safe)
    private fun outputKrillText(textId: String, textData: KrillJsonGenerator.KrillTextData) {
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

            val json = KrillJsonGenerator.generate(textData, corpusMetadata, docMetadata, includeNonWordTokens)
            
            // Choose compression format based on --lz4 flag
            val (jsonFileName, compressedData) = if (useLz4) {
                val fileName = textId.replace("_", "-").replace(".", "-") + ".json.lz4"
                val jsonBytes = json.toByteArray(Charsets.UTF_8)
                val byteOut = ByteArrayOutputStream()
                net.jpountz.lz4.LZ4FrameOutputStream(byteOut).use { lz4Out ->
                    lz4Out.write(jsonBytes)
                }
                Pair(fileName, byteOut.toByteArray())
            } else {
                // Use GZIP with level 1 compression for speed
                val fileName = textId.replace("_", "-").replace(".", "-") + ".json.gz"
                val jsonBytes = json.toByteArray(Charsets.UTF_8)
                val byteOut = ByteArrayOutputStream(jsonBytes.size)
                
                // Create GZIPOutputStream with level 1 (fast) compression
                val gzipOut = object : java.util.zip.GZIPOutputStream(byteOut) {
                    init {
                        def.setLevel(1)
                    }
                }
                gzipOut.use { it.write(jsonBytes) }
                
                Pair(fileName, byteOut.toByteArray())
            }

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

}  // End of KorapXmlTool class

enum class OutputFormat {
    CONLLU, WORD2VEC, KORAP_XML, NOW, KRILL
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

fun main(args: Array<String>): Unit {
    try { Locale.setDefault(Locale.ROOT) } catch (_: Exception) {}
    
    // Check if called as korapxml2krill for backward compatibility
    val programName = System.getProperty("sun.java.command")?.split(" ")?.first()?.split("/")?.last()
        ?: File(System.getProperty("java.class.path")).name
    
    val filteredArgs = when (programName) {
        "korapxml2krill" -> {
            // Filter out Perl-specific options and add krill format
            val perlOptions = setOf("-z", "-w", "-c")
            val newArgs = mutableListOf<String>()
            
            // Always set krill output format for korapxml2krill
            if (!args.contains("-t") && !args.contains("--to")) {
                newArgs.add("-t")
                newArgs.add("krill")
            }
            
            var i = 0
            while (i < args.size) {
                val arg = args[i]
                when {
                    perlOptions.contains(arg) -> {
                        // Skip this option
                        if (arg == "-c" && i + 1 < args.size) {
                            // Skip -c and its argument
                            i++
                        }
                    }
                    arg == "-t" || arg == "--to" -> {
                        // If format is already specified, override with krill
                        newArgs.add(arg)
                        if (i + 1 < args.size) {
                            i++
                            newArgs.add("krill")
                        }
                    }
                    else -> newArgs.add(arg)
                }
                i++
            }
            
            System.err.println("korapxml2krill compatibility mode: filtered arguments")
            newArgs.toTypedArray()
        }
        "korapxml2conllu" -> {
            // Set conllu output format for korapxml2conllu
            val newArgs = mutableListOf<String>()
            
            // Always set conllu output format
            if (!args.contains("-t") && !args.contains("--to")) {
                newArgs.add("-t")
                newArgs.add("conllu")
            }
            
            var i = 0
            while (i < args.size) {
                val arg = args[i]
                if (arg == "-t" || arg == "--to") {
                    // If format is already specified, override with conllu
                    newArgs.add(arg)
                    if (i + 1 < args.size) {
                        i++
                        newArgs.add("conllu")
                    }
                } else {
                    newArgs.add(arg)
                }
                i++
            }
            
            System.err.println("korapxml2conllu compatibility mode: using conllu format")
            newArgs.toTypedArray()
        }
        "conllu2korapxml" -> {
            // Set zip output format for conllu2korapxml (CoNLL-U → KorAP XML ZIP)
            val newArgs = mutableListOf<String>()
            
            // Always set zip output format
            if (!args.contains("-t") && !args.contains("--to")) {
                newArgs.add("-t")
                newArgs.add("zip")
            }
            
            var i = 0
            while (i < args.size) {
                val arg = args[i]
                if (arg == "-t" || arg == "--to") {
                    // If format is already specified, override with zip
                    newArgs.add(arg)
                    if (i + 1 < args.size) {
                        i++
                        newArgs.add("zip")
                    }
                } else {
                    newArgs.add(arg)
                }
                i++
            }
            
            System.err.println("conllu2korapxml mode: converting CoNLL-U to KorAP XML ZIP")
            newArgs.toTypedArray()
        }
        else -> args
    }
    
    exitProcess(CommandLine(KorapXmlTool()).execute(*filteredArgs))
}

/**
 * Debug/test entry point that doesn't call exitProcess
 */
fun debug(args: Array<String>): Int {
    try { Locale.setDefault(Locale.ROOT) } catch (_: Exception) {}
    return CommandLine(KorapXmlTool()).execute(*args)
}

