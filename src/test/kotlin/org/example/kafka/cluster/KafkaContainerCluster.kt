package org.example.kafka.cluster

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.slf4j.MDCContext
import mu.KLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.testcontainers.containers.Container
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startable
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


@Suppress("DEPRECATION")
class KafkaContainerCluster(
    imageVersion: String,
    private var brokersCount: Int,
    internalTopicsReplicationFactor: Int
) : Startable {

    companion object : KLogging() {
        private const val ZOOKEEPER_IMAGE = "confluentinc/cp-zookeeper"
        private const val KAFKA_IMAGE = "confluentinc/cp-kafka"
    }

    private var network: Network? = null
    private var zookeeper: GenericContainer<*>? = null
    private var brokers: MutableList<KafkaContainer> = mutableListOf()
    private var adminClient: AdminClient? = null
    private var producer: KafkaProducer<String, String>? = null
    private var consumer: KafkaConsumer<String, String>? = null

    init {
        require(brokersCount >= 0) {
            "brokersNum '$brokersCount' must be greater than 0"
        }
        require(!(internalTopicsReplicationFactor < 0 || internalTopicsReplicationFactor > brokersCount)) {
            "internalTopicsReplicationFactor '$internalTopicsReplicationFactor' should be less than 'brokersCount' and greater than '0'"
        }

        network = Network.newNetwork()

        zookeeper = GenericContainer(DockerImageName.parse(ZOOKEEPER_IMAGE).withTag(imageVersion))
            .withNetwork(network)
            .withNetworkAliases("zookeeper")
            .withEnv("ZOOKEEPER_CLIENT_PORT", KafkaContainer.ZOOKEEPER_PORT.toString())

        for (i in 0..<brokersCount) {
            brokers.add(
                KafkaContainer(DockerImageName.parse(KAFKA_IMAGE).withTag(imageVersion))
                    .withNetwork(network)
                    .withNetworkAliases("broker-$i")
                    .dependsOn(zookeeper)
                    .withExternalZookeeper("zookeeper:${KafkaContainer.ZOOKEEPER_PORT}")
                    .withEnv("KAFKA_BROKER_ID", i.toString() + "")
                    .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicsReplicationFactor.toString())
                    .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicsReplicationFactor.toString())
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicsReplicationFactor.toString())
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", internalTopicsReplicationFactor.toString())
                    .withStartupTimeout(Duration.ofMinutes(1))
            )
        }
    }

    fun brokers(): List<KafkaContainer> = brokers

    fun bootstrapServers(): String =
        brokers.joinToString { kafkaContainer: KafkaContainer -> kafkaContainer.bootstrapServers }

    override fun start() {
        // init and wait container startup
        zookeeper?.start()
        runBlocking {
            brokers.forEach { async(Dispatchers.IO.plus(MDCContext())) { it.start() } }
        }

        await withPollInterval Duration.ofMillis(500) atMost Duration.ofSeconds(30) untilAsserted {
            val execResult: Container.ExecResult? = zookeeper?.execInContainer(
                "sh",
                "-c",
                "zookeeper-shell zookeeper:" + KafkaContainer.ZOOKEEPER_PORT + " ls /brokers/ids | tail -n 1"
            )
            val brokers = execResult?.stdout
            logger.info { "brokers is: $brokers" }
            brokers?.split(",")?.size shouldBe brokersCount
        }

        // init admin client
        adminClient = AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers()))

        // init producer
        producer = KafkaProducer<String, String>(
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            }
        )

        // init consumer
        consumer = KafkaConsumer<String, String>(
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                put(ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID())
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            }
        )
    }

    fun createTopic(name: String, numPartitions: Int, replicationFactor: Short) {
        requireNotNull(adminClient) { "Should create topic after kafka cluster was started" }
        val topics: List<NewTopic> = listOf(NewTopic(name, numPartitions, replicationFactor))
        adminClient?.createTopics(topics)?.all()?.get(30, TimeUnit.SECONDS);
    }

    fun producer(): KafkaProducer<String, String>? {
        requireNotNull(producer) { "Should call producer after kafka cluster was started" }
        return producer
    }

    fun consumer(): KafkaConsumer<String, String>? {
        requireNotNull(consumer) { "Should call consumer after kafka cluster was started" }
        return consumer
    }

    override fun stop() {
        // stop containers
        zookeeper?.stop()
        runBlocking { brokers.forEach { async(Dispatchers.IO.plus(MDCContext())) { it.stop() } } }

        // stop admin client
        adminClient?.close()

        // stop producer
        producer?.close()

        // stop consumer
        consumer?.close()
    }
}
