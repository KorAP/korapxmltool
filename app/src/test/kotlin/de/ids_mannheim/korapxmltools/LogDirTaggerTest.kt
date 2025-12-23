package de.ids_mannheim.korapxmltools

import org.junit.Test
import java.io.File
import java.nio.file.Files
import kotlin.test.assertTrue
import picocli.CommandLine

class LogDirTaggerTest {
    @Test
    fun canSpecifyLogDirectoryForTagger() {
        // Create a proper temporary directory
        val tempDir = Files.createTempDirectory("korap-log-tagger-test").toFile()
        tempDir.deleteOnExit()
        
        // Use a known resource file
        val resource = Thread.currentThread().contextClassLoader.getResource("wud24_sample.zip") 
            ?: Thread.currentThread().contextClassLoader.getResource("wdf19.zip")
            ?: Thread.currentThread().contextClassLoader.getResource("goe.zip")
            
        val zipPath = resource?.path ?: throw RuntimeException("No suitable test zip file found")
        
        println("Using input file: $zipPath")
        
        // Use a mock tagger command to mimic behavior without needing Docker or models
        // We use 'echo' to simulate a successful tagger run that outputs valid CoNLL-U but empty or minimal
        // Actually, we just need it to start and create the log file.
        // But AnnotationWorkerPool expects valid input/output.
        // However, we can use a simpler approach:
        // Use the fake Tagger from TaggerToolBridge? 
        // No, the code path we modified is in call(), specifically when annotateWith is set.
        
        // We can use a simple command that mirrors input to output?
        // "cat" might work if we just want to verify startup logging.
        // But AnnotationWorkerPool might complain.
        
        val logDir = File(tempDir, "logs")
        
        val args = arrayOf(
            "-t", "zip",
            "-L", logDir.absolutePath,
            "-o", File(tempDir, "output.zip").absolutePath,
            "-A", "cat", 
            zipPath
        )
        
        val tool = KorapXmlTool()
        
        // Capture stderr
        val errContent = java.io.ByteArrayOutputStream()
        val originalErr = System.err
        System.setErr(java.io.PrintStream(errContent))
        
        try {
             CommandLine(tool).execute(*args)
            
            // With -o output.zip, log file should be output.log
            // With -L logDir, it should be in logDir
            val expectedLogFile = File(logDir, "output.log")
            
            println("Checking for existence of: ${expectedLogFile.absolutePath}")
            assertTrue(expectedLogFile.exists(), "Log file should exist in specified log directory: ${expectedLogFile.absolutePath}")
            
        } finally {
            System.setErr(originalErr)
            tempDir.deleteRecursively()
        }
    }
}
