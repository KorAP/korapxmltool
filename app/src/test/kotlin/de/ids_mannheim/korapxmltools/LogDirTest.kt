package de.ids_mannheim.korapxmltools

import org.junit.Test
import java.io.File
import java.nio.file.Files
import kotlin.test.assertTrue
import picocli.CommandLine

class LogDirTest {
    @Test
    fun canSpecifyLogDirectory() {
        // Create a proper temporary directory
        val tempDir = Files.createTempDirectory("korap-log-test").toFile()
        tempDir.deleteOnExit()
        
        // Use a known resource file
        // We need a ZIP file for -t krill
        val resource = Thread.currentThread().contextClassLoader.getResource("wud24_sample.zip") 
            ?: Thread.currentThread().contextClassLoader.getResource("wdf19.zip")
            ?: Thread.currentThread().contextClassLoader.getResource("goe.zip")
            
        val zipPath = resource?.path ?: throw RuntimeException("No suitable test zip file found")
        
        println("Using input file: $zipPath")
        
        // We need to use -t krill because log option mainly affects krill output logging logic
        // But krill output requires -o or -D.
        // Let's use -o to point to the temp dir as well
        val outputFile = File(tempDir, "output.krill.tar")
        
        val args = arrayOf(
            "-t", "krill",
            "-L", tempDir.absolutePath,
            "-o", outputFile.absolutePath,
            zipPath
        )
        
        val tool = KorapXmlTool()
        
        // Capture stderr
        val errContent = java.io.ByteArrayOutputStream()
        val originalErr = System.err
        System.setErr(java.io.PrintStream(errContent))
        
        try {
            // Run the tool using picocli to parse args and execute call()
            val exitCode = CommandLine(tool).execute(*args)
            
            if (exitCode != 0) {
                 println("Tool execution failed with code $exitCode")
                 println("Stderr content: ${errContent.toString()}")
            }
            
            kotlin.test.assertEquals(0, exitCode, "Tool should exit with 0. Stderr: ${errContent.toString()}")
            
            // Check if log file exists in tempDir
            // Based on logic: log file = output file .log (replacing .tar)
            // output.krill.tar -> output.krill.log
            // And with -L, it should be in tempDir.
            
            val expectedLogFile = File(tempDir, "output.krill.log")
            assertTrue(expectedLogFile.exists(), "Log file should exist in specified directory: ${expectedLogFile.absolutePath}")
            
        } finally {
            System.setErr(originalErr)
            // Clean up
            tempDir.deleteRecursively()
        }
    }
}
