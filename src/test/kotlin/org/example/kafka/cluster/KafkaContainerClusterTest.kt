package org.example.kafka.cluster

import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import mu.KLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.junit.jupiter.api.Test
import java.time.Duration

class KafkaContainerClusterTest {

    companion object : KLogging() {
        private const val TOPIC_NAME = "messages"
    }

    @Test
    fun `should run 3 kafka instance in cluster`() {
        executeInCluster { cluster -> cluster.brokers().size shouldBe 3 }
    }

    @Test
    fun `should read single message from topic`() {
        executeInCluster { cluster ->
            cluster.consumer()?.subscribe(listOf(TOPIC_NAME))

            val message = ProducerRecord(TOPIC_NAME, "1", "val")

            // async with Callback
//            cluster.producer()?.send(message) { _: RecordMetadata?, e: Exception? ->
//                if (e != null) {
//                    logger.error(e) { e.printStackTrace() }
//                } else {
//                    logger.info { "Message sent: " + message.key() }
//                }
//            }

            // sync
            cluster.producer()?.send(message)?.get()

            val record: ConsumerRecords<String, String>? = cluster.consumer()?.poll(Duration.ofSeconds(5))
            val records: MutableIterable<ConsumerRecord<String, String>>? = record?.records(TOPIC_NAME)
            records?.forEach { logger.info { "record: ${it.key()} ${it.value()}" } }

            records?.first() shouldNotBe null
            records?.first()?.key() shouldBe "1"
            records?.first()?.value() shouldBe "val"

            cluster.consumer()?.unsubscribe()
        }
    }

    private fun executeInCluster(block: (KafkaContainerCluster) -> Unit) {
        KafkaContainerCluster("6.2.1", 3, 3).use {
            it.start()
            block(it)
        }
    }
}
