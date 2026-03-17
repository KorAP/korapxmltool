package de.ids_mannheim.korapxmltools

import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertEquals

class AnnotationWorkerPoolTest {

    @Test
    fun defaultBufferedTaskUnitsScaleWithKorapXmlToolXmx() {
        val smallHeapUnits = defaultBufferedTaskUnits(
            numWorkers = 16,
            env = mapOf("KORAPXMLTOOL_XMX" to "4g"),
            runtimeMaxBytes = 4L * 1024 * 1024 * 1024
        )
        val largeHeapUnits = defaultBufferedTaskUnits(
            numWorkers = 16,
            env = mapOf("KORAPXMLTOOL_XMX" to "100g"),
            runtimeMaxBytes = 100L * 1024 * 1024 * 1024
        )

        assertEquals(512, smallHeapUnits, "4g heap should keep enough buffered work for 16 workers")
        assertTrue(largeHeapUnits >= 3200, "100g heap should allow a much larger buffered-text budget")
        assertTrue(largeHeapUnits > smallHeapUnits, "Larger KORAPXMLTOOL_XMX should increase buffered task units")
        assertEquals(4096, defaultQueuedTasks(16, largeHeapUnits), "Queue size should scale up but stay capped")
    }

    @Test
    fun pushToQueueBlocksWhenQueueCapacityIsReached() {
        val pool = AnnotationWorkerPool(
            command = "cat",
            numWorkers = 0,
            LOGGER = Logger.getLogger("AnnotationWorkerPoolTest"),
            maxQueuedTasks = 1
        )

        val firstQueued = CountDownLatch(1)
        val secondQueued = AtomicBoolean(false)

        val producer = Thread {
            pool.pushToQueue("first")
            firstQueued.countDown()
            pool.pushToQueue("second")
            secondQueued.set(true)
        }

        producer.start()

        assertTrue(firstQueued.await(2, TimeUnit.SECONDS), "First task should be queued quickly")
        Thread.sleep(200)
        assertFalse(secondQueued.get(), "Second task should block once the bounded queue is full")

        producer.interrupt()
        producer.join(2000)
        assertFalse(producer.isAlive, "Producer thread should stop after interruption")
    }
}
