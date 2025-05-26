package de.ids_mannheim.korapxmltools

import kotlinx.coroutines.*
import java.io.*
import java.lang.Thread.currentThread
import java.lang.Thread.sleep
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger

private const val BUFFER_SIZE = 10000000
private const val HIGH_WATERMARK = 1000000

class AnnotationWorkerPool(
    private val command: String,
    private val numWorkers: Int,
    private val LOGGER: Logger
) {
    private val queue: BlockingQueue<String> = LinkedBlockingQueue()
    private val threads = mutableListOf<Thread>()
    private val threadCount = AtomicInteger(0)
    private val threadsLock = Any()

    init {
        openWorkerPool()
        LOGGER.info("Annotation worker pool with ${numWorkers} threads opened")
    }

    private fun openWorkerPool() {
        repeat(numWorkers) { workerIndex ->
            Thread {
                val self = currentThread()
                var successfullyInitialized = false
                try {
                    synchronized(threadsLock) {
                        threads.add(self)
                    }
                    threadCount.incrementAndGet()
                    successfullyInitialized = true
                    LOGGER.info("Worker $workerIndex (thread ${self.id}) started.")

                    val process = ProcessBuilder("/bin/sh", "-c", command)
                        .redirectOutput(ProcessBuilder.Redirect.PIPE).redirectInput(ProcessBuilder.Redirect.PIPE)
                        .redirectError(ProcessBuilder.Redirect.INHERIT)
                        .start()

                    if (process.outputStream == null) {
                        LOGGER.severe("Worker $workerIndex (thread ${self.id}) failed to open pipe for command '$command'")
                        return@Thread // Exits thread, finally block will run
                    }
                    // Using try-with-resources for streams to ensure they are closed
                    process.outputStream.buffered(BUFFER_SIZE).use { procOutStream ->
                        process.inputStream.buffered(BUFFER_SIZE).use { procInStream ->

                            val coroutineScope = CoroutineScope(Dispatchers.IO + Job()) // Ensure Job can be cancelled
                            var inputGotEof = false // Specific to this worker's process interaction

                            // Writer coroutine
                            coroutineScope.launch {
                                val outputStreamWriter = OutputStreamWriter(procOutStream)
                                try {
                                    while (true) { // Loop until EOF is received
                                        val text = queue.poll(50, TimeUnit.MILLISECONDS) // Reduced timeout for more responsiveness
                                        if (text == null) { // Timeout, continue waiting for more data
                                            if (Thread.currentThread().isInterrupted) {
                                                LOGGER.info("Worker $workerIndex (thread ${self.id}) writer interrupted, stopping")
                                                break
                                            }
                                            continue
                                        }
                                        if (text == "#eof") {
                                            try {
                                                outputStreamWriter.write("\n# eof\n") // Send EOF to process
                                                outputStreamWriter.flush()
                                            } catch (e: IOException) {
                                                // Log error, but proceed to close
                                                LOGGER.warning("Worker $workerIndex (thread ${self.id}) failed to write EOF to process: ${e.message}")
                                            } finally {
                                                try { outputStreamWriter.close() } catch (_: IOException) {}
                                            }
                                            LOGGER.info("Worker $workerIndex (thread ${self.id}) sent EOF to process and writer is stopping.")
                                            break // Exit while loop
                                        }
                                        try {
                                            outputStreamWriter.write(text + "\n# eot\n")
                                            outputStreamWriter.flush()
                                        } catch (e: IOException) {
                                            LOGGER.severe("Worker $workerIndex (thread ${self.id}) failed to write to process: ${e.message}")
                                            break // Exit the loop
                                        }
                                    }
                                } catch (e: Exception) {
                                    LOGGER.severe("Writer coroutine in worker $workerIndex (thread ${self.id}) failed: ${e.message}")
                                }
                            }

                            // Reader coroutine
                            coroutineScope.launch {
                                val output = StringBuilder()
                                try {
                                    procInStream.bufferedReader().use { reader ->
                                        while (!inputGotEof) {
                                            if (Thread.currentThread().isInterrupted) {
                                                LOGGER.info("Worker $workerIndex (thread ${self.id}) reader interrupted, stopping")
                                                break
                                            }
                                            val line = reader.readLine()
                                            if (line == null) {
                                                if (process.isAlive) {
                                                    sleep(5) // Very short sleep when waiting for more output
                                                    continue
                                                } else {
                                                    break
                                                }
                                            }
                                            when (line) {
                                                "# eof" -> {
                                                    LOGGER.info("Worker $workerIndex (thread ${self.id}) got EOF in output")
                                                    inputGotEof = true
                                                    if (output.isNotEmpty()) {
                                                        printOutput(output.toString()) // Print any remaining output
                                                        output.clear()
                                                    }
                                                    break
                                                }
                                                "# eot" -> {
                                                    printOutput(output.toString()) // Assuming printOutput is thread-safe
                                                    output.clear()
                                                }
                                                else -> {
                                                    output.append(line).append('\n')
                                                }
                                            }
                                        }
                                    }
                                    if (output.isNotEmpty()) { // Print any remaining output
                                        printOutput(output.toString())
                                    }
                                } catch (e: Exception) {
                                    LOGGER.severe("Reader coroutine in worker $workerIndex (thread ${self.id}) failed: ${e.message}")
                                }
                            }

                            // Wait for coroutines to complete
                            try {
                                runBlocking {
                                    coroutineScope.coroutineContext[Job]?.children?.forEach { job ->
                                        job.join()
                                    }
                                }
                                LOGGER.info("Worker $workerIndex (thread ${self.id}) coroutines completed")
                            } catch (e: InterruptedException) {
                                LOGGER.info("Worker $workerIndex (thread ${self.id}) interrupted while waiting for coroutines")
                                Thread.currentThread().interrupt() // Restore interrupt status
                            } finally {
                                coroutineScope.cancel() // Ensure cleanup
                            }
                        }
                    }

                    val exitCode = process.waitFor()
                    if (exitCode != 0) {
                        LOGGER.warning("Worker $workerIndex (thread ${self.id}) process exited with code $exitCode")
                    } else {
                        LOGGER.info("Worker $workerIndex (thread ${self.id}) process finished normally")
                    }
                } catch (e: IOException) {
                    LOGGER.severe("Worker $workerIndex (thread ${self.id}) failed: ${e.message}")
                } catch (e: InterruptedException) {
                    LOGGER.info("Worker $workerIndex (thread ${self.id}) was interrupted during processing")
                    Thread.currentThread().interrupt() // Restore interrupt status
                } catch (e: Exception) { // Catch any other unexpected exceptions during setup or process handling
                    LOGGER.severe("Unhandled exception in worker thread ${self.id} (index $workerIndex): ${e.message}")
                    e.printStackTrace()
                } finally {
                    if (successfullyInitialized) {
                        synchronized(threadsLock) {
                            threads.remove(self)
                        }
                        threadCount.decrementAndGet()
                        LOGGER.info("Worker thread ${self.id} (index $workerIndex) cleaned up and exiting. Active threads: ${threadCount.get()}")
                    } else {
                        LOGGER.warning("Worker thread ${self.id} (index $workerIndex) exiting without full initialization/cleanup.")
                    }
                }
            }.start()
        }
    }


    suspend fun printOutput(output: String) {
        synchronized(System.out) {
            try {
                System.out.write(output.toByteArray())
            } catch (e: IOException) {
                LOGGER.severe("Failed to write to stdout: ${e.message}")
            }
            //  println(output)
        }
    }

    fun pushToQueue(text: String) {
        try {
            queue.put(text)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            LOGGER.warning("Interrupted while trying to push text to queue.")
        }
    }

    fun pushToQueue(texts: List<String>) {
        texts.forEach { text ->
            try {
                queue.put(text)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                LOGGER.warning("Interrupted while trying to push texts to queue. Some texts may not have been added.")
                return // Exit early if interrupted
            }
        }
    }

    fun close() {
        val currentThreadCount = threadCount.get()
        LOGGER.info("Closing worker pool with $currentThreadCount threads")
        
        // Send EOF marker for each worker - use numWorkers instead of current thread count
        // to ensure we send enough EOF markers even if some threads haven't started yet
        for (i in 0 until numWorkers) {
            try {
                queue.put("#eof")
                LOGGER.info("Sent EOF marker ${i+1}/$numWorkers to queue")
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                LOGGER.warning("Interrupted while sending EOF to workers. Some workers may not shut down cleanly.")
                break
            }
        }
        
        if (threadCount.get() > 0) {
            waitForWorkersToFinish()
        }
    }

    private fun waitForWorkersToFinish() {
        LOGGER.info("Waiting for queue to empty (current size: ${queue.size})...")
        while (queue.isNotEmpty()) {
            try {
                sleep(50) // Reduced sleep time for more responsive monitoring
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                LOGGER.warning("Interrupted while waiting for queue to empty. Proceeding to join threads.")
                break
            }
        }
        LOGGER.info("Queue is empty. Joining worker threads.")

        val threadsToJoin: List<Thread>
        synchronized(threadsLock) {
            threadsToJoin = threads.toList() // Create copy while holding lock
        }

        if (threadsToJoin.isEmpty() && threadCount.get() == 0) {
            LOGGER.info("No threads were active or recorded to join.")
        } else {
            LOGGER.info("Attempting to join ${threadsToJoin.size} thread(s) from recorded list (current active count: ${threadCount.get()}).")
            threadsToJoin.forEach { thread ->
                try {
                    thread.join(10000) // Increased timeout to 10 seconds
                    if (thread.isAlive) {
                        LOGGER.warning("Thread ${thread.id} did not terminate after 10s. Interrupting.")
                        thread.interrupt()
                        thread.join(2000) // Wait 2 seconds after interrupt
                        if (thread.isAlive) {
                            LOGGER.severe("Thread ${thread.id} failed to terminate after interrupt.")
                        }
                    }
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    LOGGER.warning("Interrupted while joining thread ${thread.id}. It may not have shut down cleanly.")
                }
            }
        }
        
        val finalCount = threadCount.get()
        if (finalCount == 0) {
            LOGGER.info("All worker threads appear to have terminated.")
        } else {
            LOGGER.warning("$finalCount worker thread(s) still marked as active according to counter. This might indicate an issue in thread lifecycle management.")
            synchronized(threadsLock) {
                if (threads.isNotEmpty()) {
                    LOGGER.warning("The internal threads list is not empty: ${threads.map { it.id }}. Forcing clear.")
                    threads.clear() // Clean up if any refs are lingering despite count issues
                }
            }
        }
    }
}


fun main() {
    val command = "cat"
    val numWorkers = 3
    val annotationWorkerPool = AnnotationWorkerPool(command, numWorkers, Logger.getLogger("de.ids_mannheim.korapxmltools.WorkerPool"))

    val texts = listOf("The", "World", "This", "Is", "A", "Test")

    annotationWorkerPool.pushToQueue(texts)

    annotationWorkerPool.close()
}
