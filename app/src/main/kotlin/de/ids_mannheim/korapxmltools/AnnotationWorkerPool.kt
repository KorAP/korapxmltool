package de.ids_mannheim.korapxmltools

import kotlinx.coroutines.*
import java.io.*
import java.lang.Thread.currentThread
import java.lang.Thread.sleep
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
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
    private var threadCount = 0

    init {
        openWorkerPool()
        LOGGER.info("Annotation worker pool with ${numWorkers} threads opened")
    }

    private fun openWorkerPool() {
        repeat(numWorkers) {
            Thread {
                try {
                    threads.add(currentThread())
                    threadCount++
                    val process = ProcessBuilder("/bin/sh", "-c", command)
                        //.directory(File("/tmp"))
                        .redirectOutput(ProcessBuilder.Redirect.PIPE).redirectInput(ProcessBuilder.Redirect.PIPE)
                        .redirectError(ProcessBuilder.Redirect.INHERIT)
                        //.redirectErrorStream(true) // Merges stderr into stdout
                        .start()
                    if (process.outputStream == null) {
                        LOGGER.severe("Worker $it failed to open pipe '$command'")
                        return@Thread
                    }
                    process.outputStream.buffered(BUFFER_SIZE)
                    process.inputStream.buffered(BUFFER_SIZE)

                    val coroutineScope = CoroutineScope(Dispatchers.IO)
                    var inputGotEof = false
                    var readBytes = 0
                    var writtenBytes = 0

                    coroutineScope.launch {
                        val outputStreamWriter = OutputStreamWriter(process.outputStream)
                        while (true) {
                            val text = queue.poll(5, TimeUnit.SECONDS)
                            if (text == "#eof" || text == null) {
                                outputStreamWriter.write("\n# eof\n")
                                outputStreamWriter.close()
                                LOGGER.info("Worker $it received eof")
                                break
                            }
                            try {
                                outputStreamWriter.write(text + "\n# eot\n")
                                /*text.split("\n\n").forEach {
                                  outputStreamWriter.write(it + "\n\n")
                                }*/
                                outputStreamWriter.flush()
                                writtenBytes += text.length
                            } catch (e: IOException) {
                                LOGGER.severe("Worker $it failed to write to process: ${e.message}")
                                threads.remove(currentThread())
                                threadCount--
                                cancel()
                            }

                        }

                    }

                    coroutineScope.launch {
                        val output = StringBuilder()
                        while (!inputGotEof && process.isAlive) {
                            process.inputStream.bufferedReader().useLines { lines ->
                                lines.forEach { line ->
                                    when (line) {
                                        "# eof" -> {
                                            LOGGER.info("Worker $it got EOF in output")
                                            inputGotEof = true
                                            return@forEach }
                                        "# eot" -> {
                                            printOutput(output.toString())
                                            output.clear() }
                                        else -> { output.append(line, "\n")
                                            readBytes += line.length +1 }
                                    }
                                }
                                printOutput(output.toString())
                                output.clear()
                                if (!inputGotEof) {
                                    LOGGER.info("Worker $it waiting for more output")
                                    sleep(10)
                                }
                            }
                        }

                    }
                    //while (!inputGotEof && process.isAlive) {
                    //    LOGGER.info("Worker $it waiting for EOF output to finish")
                    //    sleep(1000)
                   // }
                    //outputStreamWriter.close()
                    process.waitFor()
                    LOGGER.info("Worker $it finished")


                } catch (e: IOException) {
                    e.printStackTrace()
                    LOGGER.warning("Worker $it failed: ${e.message}")
                    threads.remove(currentThread())
                    threadCount--
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
        queue.offer(text)
    }

    fun pushToQueue(texts: List<String>) {
        texts.forEach {
            queue.offer(it)
        }
        // Add an "#eof" marker for each worker to know when to stop
        repeat(queue.remainingCapacity()) {
            queue.offer("#eof")
        }
    }

    fun close() {
        var n = threadCount
        LOGGER.info("Closing worker pool with $n threads")
        while (n > 0) {
            if (queue.offer("#eof")) {
                n--
            } else {
                LOGGER.info("Queue is full, waiting for workers to process")
                sleep(100)
            }
        }
        if (threadCount > 0)
            waitForWorkersToFinish()
    }

    private fun waitForWorkersToFinish() {
        while (queue.isNotEmpty()) {
            sleep(100) // Wait for queue to empty
        }
        LOGGER.info("Queue is empty, waiting for workers to finish")
        threads.forEach(Thread::join)
        LOGGER.info("All workers finished")
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
