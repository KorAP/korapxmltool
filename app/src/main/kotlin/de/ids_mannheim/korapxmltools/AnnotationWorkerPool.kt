package de.ids_mannheim.korapxmltools

import kotlinx.coroutines.*
import java.io.*
import java.lang.Thread.currentThread
import java.lang.Thread.sleep
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger

private const val BUFFER_SIZE = 10000000
private const val HIGH_WATERMARK = 1000000
private const val DEFAULT_BUFFER_HEAP_DIVISOR = 16L
private const val MIN_BUFFER_BYTES = 256L * 1024 * 1024
private const val MAX_BUFFER_BYTES = 8L * 1024 * 1024 * 1024
private const val APPROX_BYTES_PER_BUFFER_UNIT = 2L * HIGH_WATERMARK
private const val MAX_RETRIES_PER_TASK = 5 // Give up on a document after this many unsuccessful annotation attempts

internal fun parseKorapXmlToolXmxToBytes(spec: String?): Long? {
    if (spec.isNullOrBlank()) return null
    val trimmed = spec.trim()
    val match = Regex("""^(\d+)([kKmMgGtT]?)$""").matchEntire(trimmed) ?: return null
    val amount = match.groupValues[1].toLongOrNull() ?: return null
    return when (match.groupValues[2].lowercase()) {
        "" -> amount * 1024 * 1024
        "k" -> amount * 1024
        "m" -> amount * 1024 * 1024
        "g" -> amount * 1024 * 1024 * 1024
        "t" -> amount * 1024 * 1024 * 1024 * 1024
        else -> null
    }
}

internal fun annotationWorkerHeapBudgetBytes(
    env: Map<String, String> = System.getenv(),
    runtimeMaxBytes: Long = Runtime.getRuntime().maxMemory()
): Long {
    val envXmxBytes = parseKorapXmlToolXmxToBytes(env["KORAPXMLTOOL_XMX"])
    val positiveRuntimeMaxBytes = runtimeMaxBytes.takeIf { it > 0 } ?: (4L * 1024 * 1024 * 1024)
    return envXmxBytes?.coerceAtMost(positiveRuntimeMaxBytes) ?: positiveRuntimeMaxBytes
}

internal fun defaultBufferedTaskUnits(
    numWorkers: Int,
    env: Map<String, String> = System.getenv(),
    runtimeMaxBytes: Long = Runtime.getRuntime().maxMemory()
): Int {
    val heapBudgetBytes = annotationWorkerHeapBudgetBytes(env, runtimeMaxBytes)
    val targetBufferBytes = (heapBudgetBytes / DEFAULT_BUFFER_HEAP_DIVISOR)
        .coerceIn(MIN_BUFFER_BYTES, MAX_BUFFER_BYTES)
    val heapBasedUnits = (targetBufferBytes / APPROX_BYTES_PER_BUFFER_UNIT).toInt().coerceAtLeast(1)
    return maxOf(heapBasedUnits, maxOf(numWorkers * 32, 128))
}

internal fun defaultQueuedTasks(numWorkers: Int, maxBufferedTaskUnits: Int): Int {
    return maxOf(numWorkers * 64, minOf(maxBufferedTaskUnits * 2, 4096), 256)
}

class AnnotationWorkerPool(
    private val command: String,
    private val numWorkers: Int,
    private val LOGGER: Logger,
    private val outputHandler: ((String, AnnotationTask?) -> Unit)? = null,
    private val stderrLogPath: String? = null,
    // Bound buffered task text globally by approximate character count, not by document count.
    // Size the budget from KORAPXMLTOOL_XMX or the JVM max heap so large heaps can keep more
    // annotation workers busy without returning to unbounded buffering.
    private val maxBufferedTaskUnits: Int = defaultBufferedTaskUnits(numWorkers),
    private val maxQueuedTasks: Int = defaultQueuedTasks(numWorkers, maxBufferedTaskUnits)
) {
    init {
        require(maxQueuedTasks > 0) { "maxQueuedTasks must be at least 1" }
        require(maxBufferedTaskUnits > 0) { "maxBufferedTaskUnits must be at least 1" }
    }

    private val queue: BlockingQueue<AnnotationTask> = LinkedBlockingQueue(maxQueuedTasks)
    private val bufferedTaskPermits = Semaphore(maxBufferedTaskUnits, true)
    private val threads = mutableListOf<Thread>()
    private val threadCount = AtomicInteger(0)
    private val threadsLock = Any()
    private val pendingOutputHandlers = AtomicInteger(0) // Track pending outputHandler calls
    private val stderrWriter: PrintWriter? = try {
        stderrLogPath?.let { path ->
            PrintWriter(BufferedWriter(FileWriter(path, true)), true)
        }
    } catch (e: IOException) {
        LOGGER.warning("Failed to open stderr log file '$stderrLogPath': ${e.message}")
        null
    }

    data class AnnotationTask(
        val text: String,
        val docId: String?,
        val entryPath: String?,
        val bufferedUnits: Int = 0,
        val retries: Int = 0  // how many times this specific task has been re-queued after a crash
    )

    init {
        openWorkerPool()
        LOGGER.info(
            "Annotation worker pool with ${numWorkers} threads opened " +
                "(queueCapacity=$maxQueuedTasks, bufferedTaskUnits=$maxBufferedTaskUnits, " +
                "heapBudgetMB=${annotationWorkerHeapBudgetBytes() / (1024 * 1024)})"
        )
    }

    private fun unitsForText(text: String): Int {
        if (text == "#eof") return 0
        return maxOf(1, (text.length + HIGH_WATERMARK - 1) / HIGH_WATERMARK)
    }

    private fun newTask(text: String, docId: String?, entryPath: String?): AnnotationTask {
        val bufferedUnits = unitsForText(text)
        if (bufferedUnits > 0) {
            bufferedTaskPermits.acquire(bufferedUnits)
        }
        return AnnotationTask(text, docId, entryPath, bufferedUnits)
    }

    private fun releaseTaskBuffer(task: AnnotationTask?) {
        if (task != null && task.bufferedUnits > 0) {
            bufferedTaskPermits.release(task.bufferedUnits)
        }
    }

    private fun openWorkerPool() {
        repeat(numWorkers) { workerIndex ->
            Thread {
                val self = currentThread()
                var successfullyInitialized = false
                var workerAttempts = 0
                val maxRestarts = 50 // Allow up to 50 restarts per worker to handle crashes

                while (workerAttempts < maxRestarts && !Thread.currentThread().isInterrupted) {
                    workerAttempts++
                    if (workerAttempts > 1) {
                        LOGGER.info("Worker $workerIndex: Restart attempt $workerAttempts")
                    }

                try {
                    if (workerAttempts == 1) {
                        synchronized(threadsLock) {
                            threads.add(self)
                        }
                        threadCount.incrementAndGet()
                        successfullyInitialized = true
                        LOGGER.info("Worker $workerIndex (thread ${self.threadId()}) started.")
                    }

                    val process = ProcessBuilder("/bin/sh", "-c", command)
                        .redirectOutput(ProcessBuilder.Redirect.PIPE)
                        .redirectInput(ProcessBuilder.Redirect.PIPE)
                        .redirectError(ProcessBuilder.Redirect.PIPE)
                        .start()

                    if (process.outputStream == null) {
                        LOGGER.severe("Worker $workerIndex (thread ${self.threadId()}) failed to open pipe for command '$command'")
                        return@Thread // Exits thread, finally block will run
                    }

                    // pendingTasks tracks tasks already sent to the external process and awaiting output
                    val pendingTasks: BlockingQueue<AnnotationTask> = LinkedBlockingQueue()

                    // Using try-with-resources for streams to ensure they are closed
                    process.outputStream.buffered(BUFFER_SIZE).use { procOutStream ->
                        process.inputStream.buffered(BUFFER_SIZE).use { procInStream ->
                            val procErrStream = process.errorStream.buffered(BUFFER_SIZE)

                            val coroutineScope = CoroutineScope(Dispatchers.IO + Job()) // Ensure Job can be cancelled
                            var inputGotEof = false // Specific to this worker's process interaction

                            // Writer coroutine
                            coroutineScope.launch {
                                val outputStreamWriter = OutputStreamWriter(procOutStream)
                                try {
                                    while (true) { // Loop until EOF is received
                                        val task = queue.poll(50, TimeUnit.MILLISECONDS) // Reduced timeout for more responsiveness
                                        if (task == null) { // Timeout, continue waiting for more data
                                            if (Thread.currentThread().isInterrupted) {
                                                LOGGER.info("Worker $workerIndex (thread ${self.threadId()}) writer interrupted, stopping")
                                                break
                                            }
                                            continue
                                        }
                                        if (task.text == "#eof") {
                                            try {
                                                outputStreamWriter.write("# eof\n") // Send EOF to process
                                                outputStreamWriter.flush()
                                            } catch (e: IOException) {
                                                // Log error, but proceed to close
                                                LOGGER.warning("Worker $workerIndex (thread ${self.threadId()}) failed to write EOF to process: ${e.message}")
                                            } finally {
                                                try { outputStreamWriter.close() } catch (_: IOException) {}
                                            }
                                            LOGGER.info("Worker $workerIndex (thread ${self.threadId()}) sent EOF to process and writer is stopping.")
                                            break // Exit while loop
                                        }
                                        try {
                                            pendingTasks.put(task)
                                            LOGGER.fine("Worker $workerIndex: Sending ${task.text.length} chars to external process")
                                            LOGGER.finer("Worker $workerIndex: First 500 chars of data to send:\n${task.text.take(500)}")
                                            if (task.text.isNotEmpty()) {
                                                outputStreamWriter.write(task.text)
                                                if (!task.text.endsWith('\n')) {
                                                    outputStreamWriter.write('\n'.code)
                                                }
                                                outputStreamWriter.write('\n'.code)
                                            }
                                            outputStreamWriter.write("# eot\n")
                                            outputStreamWriter.flush()
                                            LOGGER.fine("Worker $workerIndex: Data sent and flushed")
                                        } catch (e: IOException) {
                                            LOGGER.severe("Worker $workerIndex (thread ${self.threadId()}) failed to write to process: ${e.message}")
                                            break // Exit the loop
                                        }
                                    }
                                } catch (e: Exception) {
                                    LOGGER.severe("Writer coroutine in worker $workerIndex (thread ${self.threadId()}) failed: ${e.message}")
                                }
                            }

                            // Reader coroutine (stdout)
                            coroutineScope.launch {
                                val output = StringBuilder()
                                var lastLineWasEmpty = false
                                var linesRead = 0
                                try {
                                    procInStream.bufferedReader().use { reader ->
                                        LOGGER.fine("Worker $workerIndex: Reader started, waiting for input from external process")
                                        while (!inputGotEof) {
                                            if (Thread.currentThread().isInterrupted) {
                                                LOGGER.info("Worker $workerIndex (thread ${self.threadId()}) reader interrupted, stopping")
                                                break
                                            }
                                            val line = reader.readLine()
                                            if (line == null) {
                                                if (process.isAlive) {
                                                    sleep(5) // Very short sleep when waiting for more output
                                                    continue
                                                } else {
                                                    LOGGER.fine("Worker $workerIndex: External process died, no more input")
                                                    break
                                                }
                                            }
                                            linesRead++
                                            when {
                                                line == "# eof" -> {
                                                    LOGGER.info("Worker $workerIndex (thread ${self.threadId()}) got EOF in output")
                                                    inputGotEof = true
                                                    if (output.isNotEmpty()) {
                                                        val task = pendingTasks.poll(500, TimeUnit.MILLISECONDS)
                                                        if (outputHandler != null) {
                                                            if (task == null) {
                                                                LOGGER.warning("Worker $workerIndex: Got # eof but no task in pendingTasks queue!")
                                                            }
                                                            LOGGER.fine("Worker $workerIndex: Invoking outputHandler with ${output.length} chars (EOF)")
                                                            pendingOutputHandlers.incrementAndGet()
                                                            try {
                                                                outputHandler.invoke(output.toString(), task)
                                                            } finally {
                                                                pendingOutputHandlers.decrementAndGet()
                                                                releaseTaskBuffer(task)
                                                            }
                                                        } else {
                                                            printOutput(output.toString())
                                                            releaseTaskBuffer(task)
                                                        }
                                                        output.clear()
                                                    }
                                                    break
                                                }
                                                line == "# eot" -> {
                                                    val task = pendingTasks.poll(500, TimeUnit.MILLISECONDS)
                                                    if (outputHandler != null) {
                                                        if (task == null) {
                                                            LOGGER.warning("Worker $workerIndex: Got # eot but no task in pendingTasks queue!")
                                                        }
                                                        LOGGER.fine("Worker $workerIndex: Invoking outputHandler with ${output.length} chars (EOT)")
                                                        pendingOutputHandlers.incrementAndGet()
                                                        try {
                                                            outputHandler.invoke(output.toString(), task)
                                                        } finally {
                                                            pendingOutputHandlers.decrementAndGet()
                                                            releaseTaskBuffer(task)
                                                        }
                                                    } else {
                                                        LOGGER.fine("Worker $workerIndex: Printing output (${output.length} chars)")
                                                        printOutput(output.toString())
                                                        releaseTaskBuffer(task)
                                                    }
                                                    output.clear()
                                                    lastLineWasEmpty = false
                                                }
                                                line.isEmpty() -> {
                                                    // Empty line - potential document separator
                                                    // In CoNLL-U, double empty line marks end of document
                                                    if (lastLineWasEmpty && output.isNotEmpty()) {
                                                        // This is the second empty line - end of document
                                                        if (outputHandler != null) {
                                                            val task = pendingTasks.poll(500, TimeUnit.MILLISECONDS)
                                                            if (task == null) {
                                                                LOGGER.warning("Worker $workerIndex: Double empty line detected but no task in pendingTasks queue!")
                                                            }
                                                            LOGGER.fine("Worker $workerIndex: Invoking outputHandler with ${output.length} chars (double empty line)")
                                                            pendingOutputHandlers.incrementAndGet()
                                                            try {
                                                                outputHandler.invoke(output.toString(), task)
                                                            } finally {
                                                                pendingOutputHandlers.decrementAndGet()
                                                                releaseTaskBuffer(task)
                                                            }
                                                            output.clear()
                                                            lastLineWasEmpty = false
                                                        } else {
                                                            // For stdout mode, just add the empty line
                                                            output.append('\n')
                                                            lastLineWasEmpty = true
                                                        }
                                                    } else {
                                                        output.append('\n')
                                                        lastLineWasEmpty = true
                                                    }
                                                }
                                                 else -> {
                                                     output.append(line).append('\n')
                                                     lastLineWasEmpty = false
                                                 }
                                            }
                                        }
                                    }
                                    if (output.isNotEmpty()) { // Print any remaining output
                                        // Only discard remaining output when the process crashed (non-zero exit
                                        // code). Annotators that don't speak the # eot/# eof protocol – like
                                        // "cat > /dev/null; cat $file" or the spacy Docker image – exit cleanly
                                        // with code 0 and rely on this path to deliver their output.
                                        val exitCode = try { process.exitValue() } catch (_: IllegalThreadStateException) { -1 }
                                        if (!inputGotEof && exitCode != 0) {
                                            // Process died mid-stream with a non-zero exit code: discard the
                                            // partial output buffer. Tasks in pendingTasks will be drained and
                                            // re-queued by the post-waitFor() logic, so they are NOT lost.
                                            LOGGER.warning("Worker $workerIndex: Discarding ${output.length} chars of partial output – process exited with code $exitCode without EOF marker; tasks will be re-queued")
                                        } else {
                                            val task = pendingTasks.poll(500, TimeUnit.MILLISECONDS)
                                            if (outputHandler != null) {
                                                if (task == null) {
                                                    LOGGER.fine("Worker $workerIndex: Remaining output but no task in pendingTasks queue!")
                                                }
                                                LOGGER.fine("Worker $workerIndex: Invoking outputHandler with ${output.length} chars (remaining)")
                                                pendingOutputHandlers.incrementAndGet()
                                                try {
                                                    outputHandler.invoke(output.toString(), task)
                                                } finally {
                                                    pendingOutputHandlers.decrementAndGet()
                                                    releaseTaskBuffer(task)
                                                }
                                            } else {
                                                printOutput(output.toString())
                                                releaseTaskBuffer(task)
                                            }
                                        }
                                    }
                                } catch (e: Exception) {
                                    LOGGER.severe("Reader coroutine in worker $workerIndex (thread ${self.threadId()}) failed: ${e.message}")
                                }
                            }

                            // Stderr reader coroutine
                            coroutineScope.launch {
                                try {
                                    procErrStream.bufferedReader().use { errReader ->
                                        var line: String?
                                        while (true) {
                                            line = errReader.readLine()
                                            if (line == null) break
                                            stderrWriter?.let { w ->
                                                synchronized(w) {
                                                    w.println("[ext-$workerIndex] $line")
                                                    w.flush()
                                                }
                                            } ?: run {
                                                LOGGER.warning("[ext-$workerIndex][stderr] $line")
                                            }
                                        }
                                    }
                                } catch (e: Exception) {
                                    LOGGER.fine("Worker $workerIndex: stderr reader finished: ${e.message}")
                                }
                            }

                            // Wait for coroutines to complete
                            try {
                                runBlocking {
                                    coroutineScope.coroutineContext[Job]?.children?.forEach { job ->
                                        job.join()
                                    }
                                }
                                LOGGER.info("Worker $workerIndex (thread ${self.threadId()}) coroutines completed")
                            } catch (e: InterruptedException) {
                                LOGGER.info("Worker $workerIndex (thread ${self.threadId()}) interrupted while waiting for coroutines")
                                Thread.currentThread().interrupt() // Restore interrupt status
                            } finally {
                                coroutineScope.cancel() // Ensure cleanup
                            }
                        }
                    }

                    val exitCode = process.waitFor()
                    if (exitCode != 0) {
                        LOGGER.warning("Worker $workerIndex (thread ${self.threadId()}) process exited with code $exitCode")

                        // Return any pending tasks back to the queue for other workers to process
                        val remainingTasks = mutableListOf<AnnotationTask>()
                        pendingTasks.drainTo(remainingTasks)
                        if (remainingTasks.isNotEmpty()) {
                            val realTasks = remainingTasks.filter { it.text != "#eof" }
                            LOGGER.warning("Worker $workerIndex: Returning ${realTasks.size} unprocessed task(s) to queue after process failure: " +
                                realTasks.joinToString(", ") { it.docId ?: "(no docId)" })
                            realTasks.forEach { task ->
                                val updated = task.copy(retries = task.retries + 1)
                                if (updated.retries > MAX_RETRIES_PER_TASK) {
                                    LOGGER.warning("Worker $workerIndex: Giving up on document after $MAX_RETRIES_PER_TASK failed attempts – skipping: docId=${task.docId}")
                                    releaseTaskBuffer(task)
                                } else {
                                    LOGGER.warning("Worker $workerIndex: Re-queuing (attempt ${updated.retries}/$MAX_RETRIES_PER_TASK): docId=${task.docId}")
                                    try {
                                        queue.put(updated)
                                    } catch (e: InterruptedException) {
                                        LOGGER.severe("Failed to return task to queue: ${e.message}")
                                        releaseTaskBuffer(task)
                                    }
                                }
                            }
                        }

                        // Check if there are more items in the queue to process
                        if (queue.isEmpty()) {
                            LOGGER.info("Worker $workerIndex: Queue is empty after crash, exiting")
                            break // Exit the restart loop
                        } else {
                            LOGGER.info("Worker $workerIndex: Restarting to process remaining ${queue.size} items in queue")
                            continue // Restart the worker
                        }
                    } else {
                        LOGGER.info("Worker $workerIndex (thread ${self.threadId()}) process finished normally")
                        break // Normal exit
                    }
                } catch (e: IOException) {
                    LOGGER.severe("Worker $workerIndex (thread ${self.threadId()}) failed: ${e.message}")
                    break // Exit restart loop on IOException
                } catch (e: InterruptedException) {
                    LOGGER.info("Worker $workerIndex (thread ${self.threadId()}) was interrupted during processing")
                    Thread.currentThread().interrupt() // Restore interrupt status
                    break // Exit restart loop on interrupt
                } catch (e: Exception) { // Catch any other unexpected exceptions during setup or process handling
                    LOGGER.severe("Unhandled exception in worker thread ${self.threadId()} (index $workerIndex): ${e.message}")
                    e.printStackTrace()
                    break // Exit restart loop on unexpected exceptions
                }
                } // End of while (workerAttempts < maxRestarts) loop

                // Cleanup after all restart attempts
                if (successfullyInitialized) {
                    synchronized(threadsLock) {
                        threads.remove(self)
                    }
                    threadCount.decrementAndGet()
                    LOGGER.info("Worker thread ${self.threadId()} (index $workerIndex) cleaned up and exiting. Active threads: ${threadCount.get()}")
                } else {
                    LOGGER.warning("Worker thread ${self.threadId()} (index $workerIndex) exiting without full initialization/cleanup.")
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

    fun pushToQueue(text: String, docId: String? = null, entryPath: String? = null) {
        var task: AnnotationTask? = null
        try {
            task = newTask(text, docId, entryPath)
            LOGGER.fine(
                "pushToQueue called: text length=${text.length}, docId=$docId, entryPath=$entryPath, " +
                    "queueSize=${queue.size}/$maxQueuedTasks, buffered=${maxBufferedTaskUnits - bufferedTaskPermits.availablePermits()}/$maxBufferedTaskUnits"
            )
            queue.put(task)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            releaseTaskBuffer(task)
            LOGGER.warning("Interrupted while trying to push text to queue.")
        }
    }

    fun pushToQueue(texts: List<String>) {
        texts.forEach { text ->
            var task: AnnotationTask? = null
            try {
                task = newTask(text, null, null)
                queue.put(task)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                releaseTaskBuffer(task)
                LOGGER.warning("Interrupted while trying to push texts to queue. Some texts may not have been added.")
                return // Exit early if interrupted
            }
        }
    }

    fun close() {
        val currentThreadCount = threadCount.get()
        val queueSizeBeforeEOF = queue.size
        LOGGER.info("Closing worker pool with $currentThreadCount threads, queue size: $queueSizeBeforeEOF")

        // Send EOF marker for each worker - use numWorkers instead of current thread count
        // to ensure we send enough EOF markers even if some threads haven't started yet
        for (i in 0 until numWorkers) {
            try {
                queue.put(AnnotationTask("#eof", null, null, 0))
                LOGGER.info("Sent EOF marker ${i+1}/$numWorkers to queue")
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                LOGGER.warning("Interrupted while sending EOF to workers. Some workers may not shut down cleanly.")
                break
            }
        }
        
        LOGGER.info("All EOF markers sent, queue size now: ${queue.size}")

        if (threadCount.get() > 0) {
            waitForWorkersToFinish()
        }
    }

    private fun waitForWorkersToFinish() {
        val startTime = System.currentTimeMillis()
        var lastReportedSize = queue.size
        LOGGER.info("Waiting for queue to empty (current size: ${queue.size})...")
        while (queue.isNotEmpty()) {
            // If all threads have exited but the queue is still non-empty, no one will ever
            // drain it (e.g. a poison document caused all workers to exhaust their retries).
            // Drain the leftovers, log the abandoned docIds, and break so the ZIP can be closed.
            if (threadCount.get() == 0) {
                val abandoned = mutableListOf<AnnotationTask>()
                queue.drainTo(abandoned)
                val realAbandoned = abandoned.filter { it.text != "#eof" && it.docId != null }
                if (realAbandoned.isNotEmpty()) {
                    LOGGER.warning("All worker threads have exited but ${realAbandoned.size} document(s) " +
                        "could not be processed and will be absent from the output:")
                    realAbandoned.forEach { task ->
                        LOGGER.warning("  Abandoned document: docId=${task.docId}")
                        releaseTaskBuffer(task)
                    }
                    // Release buffer permits for any leftover EOF sentinels too
                    abandoned.filter { it.text == "#eof" }.forEach { releaseTaskBuffer(it) }
                }
                break
            }
            try {
                sleep(50) // Reduced sleep time for more responsive monitoring
                val currentSize = queue.size
                val elapsed = (System.currentTimeMillis() - startTime) / 1000

                // Report every 5 seconds or when size changes significantly
                if (elapsed % 5 == 0L && currentSize != lastReportedSize) {
                    LOGGER.info("Queue status: $currentSize items remaining (${elapsed}s elapsed)")
                    lastReportedSize = currentSize
                }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                LOGGER.warning("Interrupted while waiting for queue to empty. Proceeding to join threads.")
                break
            }
        }
        val totalTime = (System.currentTimeMillis() - startTime) / 1000
        LOGGER.info("Queue is empty after ${totalTime}s. Joining worker threads.")

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
                    thread.join(1800000) // 30 minutes timeout - allow workers time to restart and process all documents
                    if (thread.isAlive) {
                        LOGGER.warning("Thread ${thread.threadId()} did not terminate after 30 minutes. Interrupting.")
                        thread.interrupt()
                        thread.join(10000) // Wait 10 seconds after interrupt
                        if (thread.isAlive) {
                            LOGGER.severe("Thread ${thread.threadId()} failed to terminate after interrupt.")
                        }
                    }
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    LOGGER.warning("Interrupted while joining thread ${thread.threadId()}. It may not have shut down cleanly.")
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
                    LOGGER.warning("The internal threads list is not empty: ${threads.map { it.threadId() }}. Forcing clear.")
                    threads.clear() // Clean up if any refs are lingering despite count issues
                }
            }
        }

        // CRITICAL: Wait for all pending outputHandler invocations to complete
        val pendingCount = pendingOutputHandlers.get()
        if (pendingCount > 0) {
            LOGGER.info("Waiting for $pendingCount pending outputHandler invocation(s) to complete...")
        }
        val startWait = System.currentTimeMillis()
        while (pendingOutputHandlers.get() > 0) {
            try {
                sleep(100)
                val elapsed = System.currentTimeMillis() - startWait
                if (elapsed > 30000) { // 30 second timeout
                    LOGGER.severe("Timeout waiting for ${pendingOutputHandlers.get()} pending outputHandler(s) after 30s!")
                    break
                }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                LOGGER.warning("Interrupted while waiting for pending outputHandlers")
                break
            }
        }
        if (pendingCount > 0) {
            LOGGER.info("All outputHandler invocations completed")
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
