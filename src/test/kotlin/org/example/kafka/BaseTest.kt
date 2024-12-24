package org.example.kafka

import io.kotest.matchers.shouldBe
import mu.KLogging
import org.junit.jupiter.api.Test

class BaseTest {

    companion object: KLogging()

    @Test
    fun `base test`() {
        logger.info { "Logger is working" }
        true shouldBe true
    }
}