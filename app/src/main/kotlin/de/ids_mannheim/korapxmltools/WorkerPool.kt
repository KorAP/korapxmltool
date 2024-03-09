import java.io.*
import java.lang.Thread.sleep
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.logging.Logger

class WorkerPool(private val command: String, private val numWorkers: Int, private val LOGGER: Logger) {
    private val queue: BlockingQueue<String> = LinkedBlockingQueue()
    private val threads = mutableListOf<Thread>()
    init {
        openWorkerPool()
        LOGGER.info("Worker pool opened")
    }

    private fun openWorkerPool() {
           repeat(numWorkers) {
            Thread {
                    try {
                        threads.add(Thread.currentThread())
                        val process = ProcessBuilder("/bin/sh", "-c", command)
                            //.directory(File("/tmp"))
                            .redirectOutput(ProcessBuilder.Redirect.PIPE)
                            .redirectInput(ProcessBuilder.Redirect.PIPE)
                            .redirectError(ProcessBuilder.Redirect.INHERIT)
                            //.redirectErrorStream(true) // Merges stderr into stdout
                            .start()
                        process.outputStream.buffered(1000000)
                        process.inputStream.buffered(1000000)
                        val outputStreamWriter = process.outputStream.bufferedWriter(Charsets.UTF_8)
                        val output = StringBuilder()

                        while (true) {
                            val text = queue.poll(5, TimeUnit.SECONDS)
                            if (text == "#eof" || text == null) {
                                outputStreamWriter.write("\n# eof\n")
                                outputStreamWriter.flush()
                                LOGGER.info("Worker $it received eof")
                                break
                            }

                            text.split(Regex("\n\n")).forEach {
                                outputStreamWriter.write(it + "\n\n")
                                outputStreamWriter.flush()
                                readAndPrintAvailable(process, output)
                            }
                        }

                        process.outputStream.close()
                        while(process.isAlive && output.indexOf("# eof\n") == -1) {
                            readAndPrintAvailable(process, output)
                        }
                        LOGGER.info("Worker $it got eof in output")
                        output.append(process.inputStream.bufferedReader(Charsets.UTF_8).readText())
                        synchronized(System.out) {
                            print(output.replace(Regex("\\s*\n# eof\n\\s*"), ""))
                        }

                        process.inputStream.close()

                    } catch (e: IOException) {
                        e.printStackTrace()
                        LOGGER.warning("Worker $it failed: ${e.message}")
                        threads.remove(Thread.currentThread())
                    }

            }.start()


        }
    }

    private fun readAndPrintAvailable(process: Process, output: StringBuilder) {
        if (process.inputStream.available() > 0) {
            val readBytes = ByteArray(process.inputStream.available())
            process.inputStream.read(readBytes)
            output.append(String(readBytes))
            val eotOffset = output.lastIndexOf("# eot\n")
            if (eotOffset > -1) {
                synchronized(System.out) {
                    print(output.substring(0, eotOffset).replace(Regex("\n# eot\n\\s*"), ""))
                }
                output.delete(0, eotOffset + 6)
            }
        } else {
            sleep(1)
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
        var n = threads.size
        while(n > 0) {
            if (queue.offer("#eof")) {
                n--
            } else {
                LOGGER.info("Queue is full, waiting for workers to process")
                sleep(100)
            }
        }
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
    val workerPool = WorkerPool(command, numWorkers, Logger.getLogger("WorkerPool") )

    val texts = listOf("The", "World", "This", "Is", "A", "Test")

    workerPool.pushToQueue(texts)

    workerPool.close()
}
