package de.ids_mannheim.korapxmltools

import org.junit.Test
import kotlin.test.assertTrue

class DockerTaggerTest {



    @Test
    fun testTreeTaggerArgumentAppending() {
        val tool = KorapXmlTool()
        // We need to inject the configuration manually or ensure it's initialized
        // KorapXmlTool initializes dockerTaggers in its property declaration or init block?
        // Let's assume it's available.
        
        // We can't easily call setTagWith because it's part of the picocli parsing or a method.
        // But we can check if we can access the dockerTaggers map and simulate the logic 
        // OR better: use the tool instance to parse args if possible, but picocli does that.
        
        // Actually, let's just use the fact that setTagWith is a public method (from the view_file output).
        // We need to ensure dockerTaggers is populated.
        
        // Let's try to call setTagWith directly.
        try {
            tool.setTagWith("treetagger:german:-x")
            
            // Now check the annotateWith property
            // We need to access the private property 'annotateWith' or 'dockerLogMessage'
            // If they are private, we might need reflection.
            
            val annotateWithField = KorapXmlTool::class.java.getDeclaredField("annotateWith")
            annotateWithField.isAccessible = true
            val annotateWith = annotateWithField.get(tool) as String
            
            assertTrue(annotateWith.contains("-p -x"), "Should contain both default (-p) and custom (-x) args. Output: $annotateWith")
            assertTrue(annotateWith.contains("-l german"), "Should contain model arg")
            
        } catch (e: Exception) {
            // If setTagWith fails (e.g. due to missing config), we might need to setup more.
            // But dockerTaggers seems to be statically initialized or initialized in the class.
            throw e
        }
    }
}
