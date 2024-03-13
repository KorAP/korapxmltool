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
            Level.INFO -> color = ANSI_GREEN
            Level.WARNING -> color = ANSI_YELLOW
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
    }
}
