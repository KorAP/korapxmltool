package de.ids_mannheim.korapxml2conllu

import java.net.URL
import kotlin.test.Test
import kotlin.test.assertNotNull

class AppTest {
    fun loadResource(path: String): URL {
        val resource = Thread.currentThread().contextClassLoader.getResource(path)
        requireNotNull(resource) { "Resource $path not found" }
        return resource
    }

    @Test fun appHasAGreeting() {
        val classUnderTest = App()
        val args = arrayOf(loadResource("goe.zip").path)
        assertNotNull(classUnderTest.main(args), "app should have a greeting")
    }
}
