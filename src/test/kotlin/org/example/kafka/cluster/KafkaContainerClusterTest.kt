package org.example.kafka.cluster

import io.kotest.matchers.shouldBe
import mu.KLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class KafkaContainerClusterTest {

    companion object : KLogging() {
        private const val TOPIC_NAME = "messages"
    }

    @Test
    fun `should run 3 kafka instance in cluster`() {
        executeInCluster { cluster -> cluster.brokers().size shouldBe 3 }
    }

    private fun executeInCluster(block: (KafkaContainerCluster) -> Unit) {
        KafkaContainerCluster("6.2.1", 3, 3).use {
            it.start()
            block(it)
        }
    }

    @Test
    fun test() {
        KafkaContainerCluster("6.2.1", 3, 2).use { cluster ->
            cluster.start()
            val bootstrapServers = cluster.bootstrapServers()
            logger.info { "BootstrapServers is: $bootstrapServers" }
            cluster.brokers().size shouldBe 3

            val properties = Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                put(ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID())
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            }

            val producer: KafkaProducer<String, String> = KafkaProducer<String, String>(properties)
            val consumer = KafkaConsumer<String, String>(properties)
            consumer.subscribe(listOf(TOPIC_NAME))

            val message = ProducerRecord(TOPIC_NAME, "1", "val")

            producer.send(message) { _: RecordMetadata?, e: Exception? ->
                if (e != null) {
                    e.printStackTrace()
                } else {
                    logger.info { "Message sent: " + message.key() }
                }
            }

            val record: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(1))
            val records: MutableIterable<ConsumerRecord<String, String>> = record.records(TOPIC_NAME)
            records.forEach { logger.info { "record: ${it.key()} ${it.value()}" } }
            logger.info { "ConsumerRecords: ${records}" }
            consumer.unsubscribe()
        }
    }
}
