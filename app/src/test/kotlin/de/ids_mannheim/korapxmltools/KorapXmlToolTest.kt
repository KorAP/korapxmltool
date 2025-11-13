package de.ids_mannheim.korapxmltools

import org.junit.After
import org.junit.Before
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URL
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KorapXmlToolTest {
    private val outContent = ByteArrayOutputStream(10000000)
    private val errContent = ByteArrayOutputStream()
    private val originalOut: PrintStream = System.out
    private val originalErr: PrintStream = System.err

    val goe = loadResource("goe.zip").path
    val goeSpacy = loadResource("goe.spacy.zip").path
    val goeMarmot = loadResource("goe.marmot.zip").path
    val goeTreeTagger = loadResource("goe.tree_tagger.zip").path
    val zca20scrambled = loadResource("zca20-scrambled.zip").path
    val wdf19 = loadResource("wdf19.zip").path
    val wdd17 = loadResource("wdd17sample.zip").path
    val wud24Corenlp = loadResource("wud24_sample.corenlp.zip").path

    @Before
    fun setUpStreams() {
        System.setOut(PrintStream(outContent))
        System.setErr(PrintStream(errContent))
    }

    @After
    fun restoreStreams() {
        System.setOut(originalOut)
        System.setErr(originalErr)
    }

    private fun loadResource(path: String): URL {
        val resource = Thread.currentThread().contextClassLoader.getResource(path)
        requireNotNull(resource) { "Resource $path not found" }
        return resource
    }

    @Test
    fun canConvertGOE() {
        val args = arrayOf(loadResource("goe.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "# foundry = base"
        )
        assertContains(
            outContent.toString(),
            "# start_offsets = 55 55 59 63 70 75 82 87 94 102 105 111 120 124 130 134 140 144 151 153 163 175 187 191 207 209 213 218 222 239 248 255 259 264 267 271 277 283 297 307"
        )
    }
    @Test
    fun canConvertWithMorphoAnnotations() {
        val args = arrayOf(loadResource("goe.tree_tagger.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "# foundry = tree_tagger"
        )
        assertContains(
            outContent.toString(),
            "9\tentzücke\tentzücken\t_\tVVFIN\t_\t_\t_\t_\t1.000000"
        )
    }
    @Test
    fun canInferBaseName() {
        val args = arrayOf(goeTreeTagger)
        debug(args)
        assertContains(
            outContent.toString(),
            "# foundry = tree_tagger"
        )
        assertContains(
            outContent.toString(),
            "9\tentzücke\tentzücken\t_\tVVFIN\t_\t_\t_\t_\t1.000000"
        )
    }

    @Test
    fun canConvertWfdWithMorphoAnnotations() {
        val args = arrayOf(loadResource("wdf19.tree_tagger.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "# foundry = tree_tagger"
        )
        assertContains(
            outContent.toString(),
            "\tvraie\tvrai\t_\tADJ\t_\t_\t_\t_\t"
        )
    }

    @Test
    fun canPrintHelp() {
        debug(arrayOf("-h"))
        assertContains(
            outContent.toString(),
            "--s-bounds-from-morpho"
        )
    }

    @Test
    fun respectsSiglePattern() {
        val args = arrayOf("-p",".*7", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "# text_id = WDF19_A0000.14247"
        )
        assertFalse { outContent.toString().contains("WDF19_A0000.13865") }
    }

    @Test
    fun respectsColumnsParam() {
        val args = arrayOf("-c","5", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "42\tparfaitement\t_\t_\t_\n"
        )
    }

    @Test
    fun respectsSpecial1ColumnsParam() {
        val args = arrayOf("-c","1", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "\nparfaitement\n"
        )
    }

    @Test
    fun deprecatedW2vOptionWorks() {
        val args = arrayOf("-w", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "\nje ne suis pas du tout d'accord !\n"
        )
        assertFalse { outContent.toString().contains("WDF19_A0000.13865") }
    }

    @Test
    fun w2vOptionWorks() {
        val args = arrayOf("-f", "w2v", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "\nje ne suis pas du tout d'accord !\n"
        )
        assertFalse { outContent.toString().contains("WDF19_A0000.13865") }
    }

    @Test
    fun nowOptionWorks() {
        val args = arrayOf("-f", "now", loadResource("wdf19.zip").path)
        debug(args)
        val output = outContent.toString()
        // Check that output starts with @@<text-sigle>
        assertContains(output, "@@WDF19_A0000.")
        // Check that sentence boundaries are replaced with <p> tags
        assertContains(output, " <p> ")
        // Check that it contains the expected text content
        assertContains(output, "Arts visuels Pourquoi toujours vouloir")
        // Check that it doesn't contain CoNLL-U format markers
        assertFalse(output.contains("# foundry"))
        // Check that each text is on one line (no newlines within text except at end)
        val lines = output.trim().split('\n')
        assertTrue(lines.all { it.startsWith("@@") })
    }

    @Test
    fun canConvertXMLwithInvalidComments() {
        val args = arrayOf("-w", zca20scrambled)
        debug(args)
        assertContains(
            outContent.toString(),
            "\nDys est yuch dyr Grund dyfür , dyss ys schon myl myhryry Wochyn dyuyrn kynn .\n"
        )
    }

    @Test
    fun canSetLogLevel() {
        val args = arrayOf("-l", "info", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            errContent.toString(),
            "Processing zip file"
        )
    }

    @Test
    fun canAnnotate() {
        val args = arrayOf("-A", "sed -e 's/u/x/g'", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "axtomatiqxe"
        )
        assertTrue("Annotated CoNLL-U should have at least as many lines as the original, but only has ${outContent.toString().count { it == '\n'}} lines"
        ) { outContent.toString().count { it == '\n' } >= 61511 }
    }

    @Test
    fun canExtractMetadata() {
        val args = arrayOf("--word2vec", "-m" ,"<textSigle>([^<]+)", "-m", "<creatDate>([^<]+)", loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "WDF19/A0000.12006\t2011.08.11\tmerci pour l'info je suis curieux !"
        )
    }

    @Test
    fun canHandleNonBmpText() {
        val args = arrayOf("--word2vec", wdd17)
        debug(args)
        assertContains(
            outContent.toString(),
            "\n-- mach \uD83D\uDE48 \uD83D\uDE49 \uD83D\uDE4A 20 : 45 , 1. Feb .\n" // 🙈 🙉 🙊
        )
        assertContains(
            outContent.toString(),
            "\nBereinige wenigstens die allergröbsten Sachen .\n"
        )
    }

    @Test
    fun canExtractExtraFeaturesByRegex() {
        val args = arrayOf("-e" ,"(posting/id|div/id)",loadResource("wdf19.zip").path)
        debug(args)
        assertContains(
            outContent.toString(),
            "12\t)\t_\t_\t_\t_\t_\t_\t_\t_\n" +
                    "# div/id = i.14293_8\n" +
                    "13\tDifférentiation\t_\t_\t_\t_\t_\t_\t_\t_\n" +
                    "# posting/id = i.14293_8_1\n" +
                    "14\tAinsi\t_\t_\t_\t_\t_\t_\t_\t_\n"
        )
    }

    @Test
    fun canConvertMultipleZips() {
        val args = arrayOf(wdf19, goe)
        debug(args)
        assertContains(
            outContent.toString(),
            "6\tautomatique\t_\t_\t_\t_\t_\t_\t_\t_\n"
        )
        assertContains(
            outContent.toString(),
            "36\tGedanken\t_\t_\t_\t_\t_\t_\t_\t_\n"
        )
    }

    @Test
    fun canConvertMorphoFeatureAnnotations() {
        val args = arrayOf(goeMarmot)
        debug(args)
        assertContains(
            outContent.toString(),
            "9\tentzücke\t_\t_\tVVFIN\tnumber=sg|person=3|tense=pres|mood=subj\t_\t_\t_\t_\n"
        )
    }

    @Test
    fun korapXmlOutputWorks() {
        val sourceFile = loadResource("wdf19.zip").path
        val tmpSourceFileName = java.io.File.createTempFile("tmp", ".zip").absolutePath
        File(sourceFile).copyTo(File(tmpSourceFileName), true)

        val args = arrayOf("-o", "-f", "zip", tmpSourceFileName)
        debug(args)

        val resultFile = tmpSourceFileName.toString().replace(".zip", ".base.zip")
        assert(File(resultFile).exists())
    }

    @Test
    fun overwriteWorks() {
        val sourceFile = loadResource("wdf19.zip").path
        val tmpSourceFileName = java.io.File.createTempFile("tmp", ".zip").absolutePath
        File(sourceFile).copyTo(File(tmpSourceFileName), true)
        val resultFile = tmpSourceFileName.toString().replace(".zip", ".base.zip")
        File(resultFile).createNewFile()
        val args = arrayOf("-o", "-f", "zip", tmpSourceFileName)
        debug(args)
        assert(File(resultFile).exists())
        assert(File(resultFile).length() > 0)
     }

    @Test
    fun canWord2VecLemma() {
        val args = arrayOf("--lemma", "-f", "w2v", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        // Expect lemma sequence containing "mein Ankunft" (surface would include inflected form elsewhere)
        assertContains(out, " mein Ankunft ")
    }

    @Test
    fun canNowLemma() {
        val args = arrayOf("--lemma", "-f", "now", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        assertContains(out, "@@")
        assertContains(out, " <p> ")
        assertContains(out, " mein Ankunft ")
    }

    @Test
    fun lemmaOnlyWord2VecWorks() {
        val args = arrayOf("--lemma-only", "-f", "w2v", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        // Should produce some lemma tokens without requiring data.xml
        assertTrue(out.contains(" mein ") || out.contains(" Ankunft "))
    }

    @Test
    fun lemmaOnlyNowWorks() {
        val args = arrayOf("--lemma-only", "-f", "now", loadResource("goe.tree_tagger.zip").path)
        debug(args)
        val out = outContent.toString()
        assertContains(out, "@@")
        assertContains(out, " <p> ")
    }

    @Test
    fun excludeZipGlobSkipsFiles() {
        val args = arrayOf("--exclude-zip-glob", "goe.zip", loadResource("wdf19.zip").path, loadResource("goe.zip").path)
        debug(args)
        val out = outContent.toString()
        // Expect French content, but not the German token from GOE
        assertContains(out, "automatique")
        assertFalse(out.contains("Gedanken"))
    }

    @Test
    fun sequentialOnlyForNowAndW2V() {
        val args = arrayOf("--sequential", loadResource("wdf19.zip").path)
        // Default format is conllu; this should error
        val rc = debug(args)
        // Non-zero is expected; and error message should be present
        assertTrue(rc != 0)
        assertContains(errContent.toString(), "--sequential is supported only with -f word2vec or -f now")
    }

    @Test
    fun dependencyColumnsArePopulatedFromSpacyZip() {
        val args = arrayOf(goeSpacy)
        debug(args)
        val out = outContent.toString()

        // Check that output is CoNLL-U format
        assertContains(out, "# foundry = spacy")
        assertContains(out, "# text_id = GOE_AGA.00000")

        // Get data lines (non-comment, non-empty)
        val dataLines = out.lines()
            .filter { !it.startsWith("#") && it.isNotBlank() }

        assertTrue(dataLines.isNotEmpty(), "Should have data lines in output")

        // Parse tokens and check dependency columns (column 7 = HEAD, column 8 = DEPREL)
        var tokensWithHead = 0
        var tokensWithDeprel = 0
        var totalTokens = 0

        for (line in dataLines) {
            val columns = line.split(Regex("\\s+"))
            if (columns.size >= 8) {
                totalTokens++
                // Column 7 (index 6) is HEAD, column 8 (index 7) is DEPREL
                val head = columns[6]
                val deprel = columns[7]

                if (head != "_") tokensWithHead++
                if (deprel != "_") tokensWithDeprel++
            }
        }

        // Assert that we have tokens
        assertTrue(totalTokens > 0, "Should have parsed at least some tokens")

        // Print diagnostic information
        System.err.println("=== Dependency Test Diagnostics ===")
        System.err.println("Total tokens: $totalTokens")
        System.err.println("Tokens with HEAD (!= '_'): $tokensWithHead")
        System.err.println("Tokens with DEPREL (!= '_'): $tokensWithDeprel")
        System.err.println("First 5 data lines:")
        dataLines.take(5).forEach { System.err.println("  $it") }

        // Assert that HEAD column (col 7) is populated for a significant portion of tokens
        // When processing spacy zip alone, we get ~50% coverage (base tokens don't have deps)
        val headCoverage = (tokensWithHead.toDouble() / totalTokens) * 100
        assertTrue(
            headCoverage > 40.0,
            "HEAD column should be populated for significant portion of tokens. Found: $tokensWithHead/$totalTokens (${headCoverage}%)"
        )

        // Assert that DEPREL column (col 8) is populated for a significant portion of tokens
        val deprelCoverage = (tokensWithDeprel.toDouble() / totalTokens) * 100
        assertTrue(
            deprelCoverage > 40.0,
            "DEPREL column should be populated for significant portion of tokens. Found: $tokensWithDeprel/$totalTokens (${deprelCoverage}%)"
        )

        // Check for specific dependency relations and head indices in output
        // Look for numeric head indices (not "_")
        assertTrue(
            out.contains(Regex("\\n\\d+\\t\\S+\\t\\S+\\t\\S+\\t\\S+\\t\\S+\\t\\d+\\t\\S+\\t")),
            "Should find tokens with numeric HEAD values in column 7"
        )
    }

    @Test
    fun krillOutputMatchesExpectedStructure() {
        // Test krill format output generation succeeds
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path
        val marmotMaltZip = loadResource("wud24_sample.marmot-malt.zip").path
        val opennlpZip = loadResource("wud24_sample.opennlp.zip").path
        val treeTaggerZip = loadResource("wud24_sample.tree_tagger.zip").path

        // Create temporary output directory
        val tempDir = File.createTempFile("krill_test", "").let {
            it.delete()
            it.mkdirs()
            it
        }

        try {
            // Generate krill output to temp directory
            val args = arrayOf("-f", "krill", "-D", tempDir.path, baseZip, spacyZip, marmotMaltZip, opennlpZip, treeTaggerZip)
            val exitCode = debug(args)

            // Check that generation succeeded
            assertTrue(exitCode == 0, "Krill conversion should succeed")

            // Expected output file name
            val generatedTar = File(tempDir, "wud24_sample.krill.tar")
            assertTrue(generatedTar.exists(), "Generated krill tar should exist at ${generatedTar.path}")
            assertTrue(generatedTar.length() > 0, "Generated tar should not be empty")

            // Extract tar to verify it contains JSON files
            val extractDir = File.createTempFile("extract", "").let {
                it.delete()
                it.mkdirs()
                it
            }

            try {
                // Extract tar
                val tarProcess = ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path)
                    .redirectErrorStream(true)
                    .start()
                assertTrue(tarProcess.waitFor() == 0, "Tar extraction should succeed")

                // Get list of JSON files
                val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
                assertTrue(jsonFiles.isNotEmpty(), "Tar should contain JSON.gz files")

                // Verify each JSON file is valid
                jsonFiles.forEach { jsonFile ->
                    val jsonContent = ProcessBuilder("gunzip", "-c", jsonFile.path)
                        .redirectOutput(ProcessBuilder.Redirect.PIPE)
                        .start()
                        .inputStream
                        .bufferedReader()
                        .readText()

                    // Check required fields in JSON
                    assertTrue(jsonContent.contains("\"@context\""), "JSON should have @context")
                    assertTrue(jsonContent.contains("\"@type\":\"koral:corpus\""), "JSON should have correct @type")
                    assertTrue(jsonContent.contains("\"data\""), "JSON should have data section")
                    assertTrue(jsonContent.contains("\"foundries\""), "JSON should have foundries")
                    assertTrue(jsonContent.contains("\"layerInfos\""), "JSON should have layerInfos")
                    assertTrue(jsonContent.contains("\"name\":\"tokens\""), "JSON should have name field")
                    assertTrue(jsonContent.contains("\"stream\""), "JSON should have stream")
                    assertTrue(jsonContent.contains("\"text\""), "JSON should have text")

                    // Check for multiple foundries
                    assertTrue(jsonContent.contains("spacy"), "JSON should contain spacy foundry")
                    assertTrue(jsonContent.contains("marmot") || jsonContent.contains("malt"), "JSON should contain marmot or malt foundry")
                    assertTrue(jsonContent.contains("treetagger"), "JSON should contain treetagger foundry")
                }
            } finally {
                extractDir.deleteRecursively()
            }
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @Test
    fun krillOutputContainsInverseDependencies() {
        // Test that inverse dependency annotations are included
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path

        val tempDir = File.createTempFile("krill_inverse_test", "").let {
            it.delete()
            it.mkdirs()
            it
        }

        try {
            val args = arrayOf("-f", "krill", "-D", tempDir.path, baseZip, spacyZip)
            val exitCode = debug(args)
            assertTrue(exitCode == 0, "Krill conversion should succeed")

            val generatedTar = File(tempDir, "wud24_sample.krill.tar")
            assertTrue(generatedTar.exists())

            // Extract and check for inverse dependencies
            val extractDir = File.createTempFile("extract_inv", "").let {
                it.delete()
                it.mkdirs()
                it
            }

            try {
                ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path).start().waitFor()
                val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
                assertTrue(jsonFiles.isNotEmpty())

                jsonFiles.forEach { jsonFile ->
                    val jsonContent = ProcessBuilder("gunzip", "-c", jsonFile.path)
                        .redirectOutput(ProcessBuilder.Redirect.PIPE)
                        .start()
                        .inputStream
                        .bufferedReader()
                        .readText()

                    // Check for inverse dependency annotations (format: <:foundry/d:label$...)
                    assertTrue(
                        jsonContent.contains("<:") && jsonContent.contains("/d:"),
                        "JSON should contain inverse dependency annotations"
                    )
                }
            } finally {
                extractDir.deleteRecursively()
            }
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @Test
    fun krillOutputContainsBaseStructureSpans() {
        // Test that base structure spans are included
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path

        val tempDir = File.createTempFile("krill_base_test", "").let {
            it.delete()
            it.mkdirs()
            it
        }

        try {
            val args = arrayOf("-f", "krill", "-D", tempDir.path, baseZip, spacyZip)
            val exitCode = debug(args)
            assertTrue(exitCode == 0, "Krill conversion should succeed")

            val generatedTar = File(tempDir, "wud24_sample.krill.tar")
            assertTrue(generatedTar.exists())

            val extractDir = File.createTempFile("extract_base", "").let {
                it.delete()
                it.mkdirs()
                it
            }

            try {
                ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path).start().waitFor()
                val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
                assertTrue(jsonFiles.isNotEmpty())

                jsonFiles.forEach { jsonFile ->
                    val jsonContent = ProcessBuilder("gunzip", "-c", jsonFile.path)
                        .redirectOutput(ProcessBuilder.Redirect.PIPE)
                        .start()
                        .inputStream
                        .bufferedReader()
                        .readText()

                    // Check for base structure spans
                    assertTrue(
                        jsonContent.contains("base/s:t"),
                        "JSON should contain base text span (base/s:t)"
                    )
                    assertTrue(
                        jsonContent.contains("base/s:s"),
                        "JSON should contain base sentence spans (base/s:s)"
                    )
                }
            } finally {
                extractDir.deleteRecursively()
            }
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @Test
    fun krillOutputIncludesAllFoundries() {
        // Test that all foundries are properly included
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path
        val marmotZip = loadResource("wud24_sample.marmot-malt.zip").path
        val opennlpZip = loadResource("wud24_sample.opennlp.zip").path
        val treeTaggerZip = loadResource("wud24_sample.tree_tagger.zip").path

        val tempDir = File.createTempFile("krill_foundries_test", "").let {
            it.delete()
            it.mkdirs()
            it
        }

        try {
            val args = arrayOf("-f", "krill", "-D", tempDir.path, baseZip, spacyZip, marmotZip, opennlpZip, treeTaggerZip)
            val exitCode = debug(args)
            assertTrue(exitCode == 0, "Krill conversion should succeed")

            val generatedTar = File(tempDir, "wud24_sample.krill.tar")
            assertTrue(generatedTar.exists())

            val extractDir = File.createTempFile("extract_foundries", "").let {
                it.delete()
                it.mkdirs()
                it
            }

            try {
                ProcessBuilder("tar", "-xf", generatedTar.path, "-C", extractDir.path).start().waitFor()
                val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") } ?: emptyList()
                assertTrue(jsonFiles.isNotEmpty())

                jsonFiles.forEach { jsonFile ->
                    val jsonContent = ProcessBuilder("gunzip", "-c", jsonFile.path)
                        .redirectOutput(ProcessBuilder.Redirect.PIPE)
                        .start()
                        .inputStream
                        .bufferedReader()
                        .readText()

                    // Check foundries field includes all expected foundries
                    val foundries = jsonContent.substringAfter("\"foundries\":").substringBefore(",").trim()
                    assertTrue(foundries.contains("spacy"), "Foundries should include spacy")
                    assertTrue(foundries.contains("marmot") || foundries.contains("malt"), "Foundries should include marmot or malt")
                    assertTrue(foundries.contains("opennlp"), "Foundries should include opennlp")
                    assertTrue(foundries.contains("treetagger"), "Foundries should include treetagger (not tt)")
                    assertTrue(foundries.contains("dereko"), "Foundries should include dereko")
                }
            } finally {
                extractDir.deleteRecursively()
            }
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @Test
    fun krillRespectsNonWordTokenOption() {
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path

        val defaultDir = File.createTempFile("krill_default", "").let {
            it.delete()
            it.mkdirs()
            it
        }

        try {
            val defaultArgs = arrayOf("-f", "krill", "-D", defaultDir.path, baseZip, spacyZip, wud24Corenlp)
            val defaultExit = debug(defaultArgs)
            assertTrue(defaultExit == 0, "Krill conversion should succeed without --non-word-tokens")

            val defaultTar = File(defaultDir, "wud24_sample.krill.tar")
            assertTrue(defaultTar.exists(), "Default krill tar should exist")

            val defaultJsons = readKrillJson(defaultTar).values
            assertTrue(defaultJsons.isNotEmpty(), "Default Krill tar should contain JSON files")
            assertTrue(
                defaultJsons.all { !it.contains("\"s:,\"") },
                "Default Krill output should skip comma tokens"
            )
            assertTrue(
                defaultJsons.all { !it.contains("\"s:!\"") },
                "Default Krill output should skip exclamation mark tokens"
            )
        } finally {
            defaultDir.deleteRecursively()
        }

        val flagDir = File.createTempFile("krill_nwt", "").let {
            it.delete()
            it.mkdirs()
            it
        }

        try {
            val flagArgs = arrayOf("-f", "krill", "--non-word-tokens", "-D", flagDir.path, baseZip, spacyZip, wud24Corenlp)
            val flagExit = debug(flagArgs)
            assertTrue(flagExit == 0, "Krill conversion should succeed with --non-word-tokens")

            val flagTar = File(flagDir, "wud24_sample.krill.tar")
            assertTrue(flagTar.exists(), "Krill tar should exist when --non-word-tokens is set")

            val flagJsons = readKrillJson(flagTar).values
            assertTrue(flagJsons.isNotEmpty(), "Krill tar should contain JSON files when --non-word-tokens is set")
            assertTrue(
                flagJsons.any { it.contains("\"s:,\"") },
                "Krill output should include commas when --non-word-tokens is set"
            )
            assertTrue(
                flagJsons.any { it.contains("\"s:!\"") },
                "Krill output should include exclamation marks when --non-word-tokens is set"
            )
        } finally {
            flagDir.deleteRecursively()
        }
    }

    @Test
    fun krillDefaultMatchesPerlReference() {
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path
        val marmotMaltZip = loadResource("wud24_sample.marmot-malt.zip").path
        val opennlpZip = loadResource("wud24_sample.opennlp.zip").path
        val treeTaggerZip = loadResource("wud24_sample.tree_tagger.zip").path
        val corenlpZip = wud24Corenlp
        val referenceTar = File(loadResource("wud24_sample.wonwtopt.krill.tar").toURI())
        assertTrue(referenceTar.exists(), "Reference Krill tar is missing: ${referenceTar.path}")

        val kotlinDir = File.createTempFile("krill_reference_cmp", "").let {
            it.delete()
            it.mkdirs()
            it
        }

        try {
            val args = arrayOf(
                "-f", "krill",
                "-D", kotlinDir.path,
                baseZip,
                spacyZip,
                marmotMaltZip,
                treeTaggerZip,
                corenlpZip
            )
            val exitCode = debug(args)
            assertTrue(exitCode == 0, "Krill conversion should succeed for reference comparison")

            val kotlinTar = File(kotlinDir, "wud24_sample.krill.tar")
            assertTrue(kotlinTar.exists(), "Kotlin-produced Krill tar should exist at ${kotlinTar.path}")

            val kotlinJsons = readKrillJson(kotlinTar)
            val referenceJsons = readKrillJson(referenceTar)

            assertEquals(referenceJsons.keys, kotlinJsons.keys, "Kotlin and reference JSON sets differ")

            val tokensToCheck = listOf("\"s:,\"", "\"s:.\"")
            referenceJsons.forEach { (doc, referenceJson) ->
                val kotlinJson = kotlinJsons.getValue(doc)
                tokensToCheck.forEach { token ->
                    val refHas = referenceJson.contains(token)
                    val kotlinHas = kotlinJson.contains(token)
                    assertEquals(
                        refHas,
                        kotlinHas,
                        "Mismatch for $token in document $doc compared to reference"
                    )
                }
            }
        } finally {
            kotlinDir.deleteRecursively()
        }
    }

    @Test
    fun krillNonWordTokensMatchesPerlReference() {
        val baseZip = loadResource("wud24_sample.zip").path
        val spacyZip = loadResource("wud24_sample.spacy.zip").path
        val marmotMaltZip = loadResource("wud24_sample.marmot-malt.zip").path
        val treeTaggerZip = loadResource("wud24_sample.tree_tagger.zip").path
        val corenlpZipNwt = wud24Corenlp
        val referenceTar = File(loadResource("wud24_sample.nwt.krill.tar").toURI())
        assertTrue(referenceTar.exists(), "Non-word-token reference tar missing: ${referenceTar.path}")

        val kotlinDir = File.createTempFile("krill_reference_nwt", "").let {
            it.delete()
            it.mkdirs()
            it
        }

        try {
            val args = arrayOf(
                "-f", "krill",
                "--non-word-tokens",
                "-D", kotlinDir.path,
                baseZip,
                spacyZip,
                marmotMaltZip,
                treeTaggerZip,
                corenlpZipNwt
            )
            val exitCode = debug(args)
            assertTrue(exitCode == 0, "Krill conversion with --non-word-tokens should succeed for reference comparison")

            val kotlinTar = File(kotlinDir, "wud24_sample.krill.tar")
            assertTrue(kotlinTar.exists(), "Kotlin-produced Krill tar (nwt) should exist at ${kotlinTar.path}")

            val kotlinJsons = readKrillJson(kotlinTar)
            val referenceJsons = readKrillJson(referenceTar)

            assertEquals(referenceJsons.keys, kotlinJsons.keys, "Kotlin and reference JSON sets differ (nwt)")

            val tokensToCheck = listOf(
                "\"s:,\"",
                "\"s:.\"",
                "\"s:!\"",
                "\"marmot/p:\\$,\"",
                "\"spacy/p:\\$,\"",
                "\"tt/p:\\$,\"",
                "\"-:corenlp/sentences\$<i>11\"",
                "corenlp/s=spans",
                "corenlp/c=spans"
            )
            referenceJsons.forEach { (doc, referenceJson) ->
                val kotlinJson = kotlinJsons.getValue(doc)
                tokensToCheck.forEach { token ->
                    val refHas = referenceJson.contains(token)
                    val kotlinHas = kotlinJson.contains(token)
                    assertEquals(
                        refHas,
                        kotlinHas,
                        "Mismatch for $token in document $doc compared to nwt reference"
                    )
                }
            }
        } finally {
            kotlinDir.deleteRecursively()
        }
    }

    private fun readKrillJson(tarFile: File): Map<String, String> {
        val extractDir = File.createTempFile("krill_extract", "").let {
            it.delete()
            it.mkdirs()
            it
        }

        return try {
            val tarProcess = ProcessBuilder("tar", "-xf", tarFile.path, "-C", extractDir.path)
                .redirectErrorStream(true)
                .start()
            assertTrue(tarProcess.waitFor() == 0, "Tar extraction should succeed for ${tarFile.path}")

            val jsonFiles = extractDir.listFiles()?.filter { it.name.endsWith(".json.gz") }.orEmpty()
            assertTrue(jsonFiles.isNotEmpty(), "No JSON files found in ${tarFile.path}")

            jsonFiles.associate { jsonFile ->
                val jsonContent = ProcessBuilder("gunzip", "-c", jsonFile.path)
                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                    .start()
                    .inputStream
                    .bufferedReader()
                    .use { it.readText() }
                jsonFile.name to jsonContent
            }
        } finally {
            extractDir.deleteRecursively()
        }
    }
}
