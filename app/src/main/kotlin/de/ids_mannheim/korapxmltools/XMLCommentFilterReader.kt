package de.ids_mannheim.korapxmltools

import sun.nio.cs.StreamDecoder
import java.io.InputStream
import java.io.InputStreamReader

class XMLCommentFilterReader(`in`: InputStream, private val charsetName: String) : InputStreamReader(`in`, charsetName) {
    companion object {
        const val COMMENT_START = "<!--"
        const val COMMENT_END = "-->"
    }

    private var commentBuffer: StringBuilder? = null

    override fun read(cbuf: CharArray, off: Int, len: Int): Int {
        var bytesRead = super.read(cbuf, off, len)
        if (bytesRead <= 0) {
            return bytesRead
        }

        val filteredBuffer = StringBuilder()
        var currentIndex = off
        var isFiltered = false

        while (currentIndex < off + bytesRead) {
            val currentChar = cbuf[currentIndex]
            if (commentBuffer != null) {
                if (currentChar == COMMENT_END[commentBuffer!!.length]) {
                    commentBuffer!!.append(currentChar)
                    if (commentBuffer!!.endsWith(COMMENT_END)) {
                        commentBuffer = null // End of comment
                    }
                } else {
                    commentBuffer!!.clear()
                }
            } else if (currentChar == COMMENT_START[0]) {
                // Check if starting a comment
                val peekBuffer = StringBuilder()
                var peekIndex = currentIndex
                while (peekIndex < off + bytesRead && peekBuffer.length < COMMENT_START.length) {
                    peekBuffer.append(cbuf[peekIndex++])
                }
                if (peekBuffer.toString() == COMMENT_START) {
                    isFiltered = true
                    commentBuffer = StringBuilder()
                    currentIndex = peekIndex // Skip ahead
                    continue // Continue without appending the current character
                }
                filteredBuffer.append(currentChar)
            } else {
                filteredBuffer.append(currentChar)
            }
            currentIndex++
        }
        if (!isFiltered) {
            return bytesRead
        }
        val filterdString = filteredBuffer.toString()// Copy filtered characters to the original buffer
        val filteredChars = filterdString.toCharArray()
        System.arraycopy(filteredChars, 0, cbuf, off, filteredChars.size)
        bytesRead = filteredChars.size
        return bytesRead
    }

    override fun read(): Int {
        // Not implemented, you should use read(char[], int, int) instead
        throw UnsupportedOperationException("read() is not supported. Use read(char[], int, int) instead.")
    }

    override fun close() {
        super.close()
        commentBuffer = null // Reset the comment buffer on close
    }
}
