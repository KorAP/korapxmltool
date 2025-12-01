package de.ids_mannheim.korapxmltools

import org.junit.Test
import kotlin.test.assertTrue
import kotlin.test.assertFalse

class DockerTaggerTest {

    @Test
    fun testSpacyTaggerConfiguration() {
        val tool = KorapXmlTool()
        
        // Test spaCy tagging (should add -d flag to disable parsing)
        tool.setTagWith("spacy")
        
        val annotateWithField = KorapXmlTool::class.java.getDeclaredField("annotateWith")
        annotateWithField.isAccessible = true
        val annotateWith = annotateWithField.get(tool) as String
        
        assertTrue(annotateWith.contains("-d"), "spaCy tagger should contain -d flag to disable parsing. Output: $annotateWith")
        assertTrue(annotateWith.contains("-m de_core_news_lg"), "Should contain default model")
        assertTrue(annotateWith.contains("korap/conllu-spacy"), "Should use correct Docker image")
    }

    @Test
    fun testSpacyParserConfiguration() {
        val tool = KorapXmlTool()
        
        // Test spaCy parsing (should NOT add -d flag)
        tool.setParseWith("spacy")
        
        val annotateWithField = KorapXmlTool::class.java.getDeclaredField("annotateWith")
        annotateWithField.isAccessible = true
        val annotateWith = annotateWithField.get(tool) as String
        
        assertFalse(annotateWith.contains("-d"), "spaCy parser should NOT contain -d flag. Output: $annotateWith")
        assertTrue(annotateWith.contains("-m de_core_news_lg"), "Should contain default model")
        assertTrue(annotateWith.contains("korap/conllu-spacy"), "Should use correct Docker image")
    }

    @Test
    fun testSpacyCustomModel() {
        val tool = KorapXmlTool()
        
        // Test spaCy with custom model
        tool.setTagWith("spacy:de_core_news_sm")
        
        val annotateWithField = KorapXmlTool::class.java.getDeclaredField("annotateWith")
        annotateWithField.isAccessible = true
        val annotateWith = annotateWithField.get(tool) as String
        
        assertTrue(annotateWith.contains("-m de_core_news_sm"), "Should contain custom model. Output: $annotateWith")
        assertTrue(annotateWith.contains("-d"), "Should still contain -d flag for tagging")
    }

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
