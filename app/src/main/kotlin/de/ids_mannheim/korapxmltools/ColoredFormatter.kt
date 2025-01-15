package de.ids_mannheim.korapxmltools

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.logging.Formatter
import java.util.logging.Level
import java.util.logging.LogRecord


class ColoredFormatter : Formatter() {

    override fun format(record: LogRecord): String {
        var color = ""

        // Set color based on log level
        when(record.level) {
            Level.SEVERE -> color = ANSI_RED
            Level.CONFIG -> color = ANSI_BLUE
            Level.INFO -> color = ANSI_GREEN
            Level.WARNING -> color = ANSI_YELLOW
            Level.FINE -> color = ANSI_DARK_GRAY
            Level.FINER -> color = ANSI_MEDIUM_GRAY
            Level.FINEST -> color = ANSI_LIGHT_GRAY
        }

        return "${color}${dateTimeFormatter.format(Instant.now())} [${record.level.name.padStart(7)}] ${formatMessage(record)}${ANSI_RESET}\n"
    }

    companion object {
        private val dateTimeFormatter: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss").withZone(ZoneId.systemDefault())

        // ANSI color codes
        private const val ANSI_RESET = "\u001B[0m"
        private const val ANSI_RED = "\u001B[31m"
        private const val ANSI_GREEN = "\u001B[32m"
        private const val ANSI_YELLOW = "\u001B[33m"
        private const val ANSI_BLUE = "\u001B[34m"
        private const val ANSI_PURPLE = "\u001B[35m"
        private const val ANSI_CYAN = "\u001B[36m"
        private const val ANSI_WHITE = "\u001B[37m"
        private const val ANSI_DARK_GRAY = "\u001B[38;5;240m"
        private const val ANSI_MEDIUM_GRAY = "\u001B[38;5;245m"
        private const val ANSI_LIGHT_GRAY = "\u001B[38;5;250m"
    }
}
