package de.ids_mannheim.korapxmltools

class NonBmpString : CharSequence {

    private val utf32Chars: IntArray

    constructor(input: String) {
        utf32Chars = input.toUtf32Array()
    }

    constructor(utf32Chars: IntArray) {
        this.utf32Chars = utf32Chars.copyOf()
    }

    override val length: Int
        get() = utf32Chars.size

    override fun get(index: Int): Char {
        if (index < 0 || index >= length) {
            throw IndexOutOfBoundsException("Index $index is out of bounds for NonBmpString with length $length")
        }
        val codePoint = utf32Chars[index]
        return if (Character.isBmpCodePoint(codePoint)) {
            codePoint.toChar()
        } else {
            throw UnsupportedOperationException("Non-BMP characters not supported directly as Char")
        }
    }

    override fun subSequence(startIndex: Int, endIndex: Int): CharSequence {
        if (startIndex < 0 || endIndex > length || startIndex > endIndex) {
            throw IndexOutOfBoundsException("Invalid substring range")
        }
        val subArray = utf32Chars.copyOfRange(startIndex, endIndex)
        return NonBmpString(subArray)
    }

    override fun toString(): String {
        val stringBuilder = StringBuilder()
        utf32Chars.forEach { codePoint ->
            if (Character.isBmpCodePoint(codePoint)) {
                stringBuilder.append(codePoint.toChar())
            } else {
                stringBuilder.append(Character.highSurrogate(codePoint))
                stringBuilder.append(Character.lowSurrogate(codePoint))
            }
        }
        return stringBuilder.toString()
    }

    fun appendRangeTo(sb: StringBuilder, startIndex: Int, endIndex: Int) {
        if (startIndex < 0 || endIndex > length || startIndex > endIndex) {
            throw IndexOutOfBoundsException("Invalid range $startIndex..$endIndex for NonBmpString length $length")
        }
        var i = startIndex
        while (i < endIndex) {
            val cp = utf32Chars[i]
            if (Character.isBmpCodePoint(cp)) {
                sb.append(cp.toChar())
            } else {
                sb.append(Character.highSurrogate(cp))
                sb.append(Character.lowSurrogate(cp))
            }
            i++
        }
    }

    private fun String.toUtf32Array(): IntArray {
        val codePoints = IntArray(Character.codePointCount(this, 0, length))
        var index = 0
        var offset = 0
        while (offset < length) {
            val codePoint = Character.codePointAt(this, offset)
            codePoints[index++] = codePoint
            offset += Character.charCount(codePoint)
        }
        return codePoints
    }
}
