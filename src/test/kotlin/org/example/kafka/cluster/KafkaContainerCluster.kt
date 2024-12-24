package org.example.kafka.cluster

import mu.KLogging
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startable
import org.testcontainers.utility.DockerImageName
import java.time.Duration


@Suppress("DEPRECATION")
class KafkaContainerCluster(
    imageVersion: String,
    private var brokersCount: Int,
    internalTopicsRf: Int
) : Startable {

    companion object : KLogging() {
        private const val ZOOKEEPER_IMAGE = "confluentinc/cp-zookeeper"
        private const val KAFKA_IMAGE = "confluentinc/cp-kafka"
    }

    private var network: Network? = null
    private var zookeeper: GenericContainer<*>? = null
    private var brokers: MutableList<KafkaContainer> = mutableListOf()

    init {
        require(brokersCount >= 0) {
            "brokersNum '$brokersCount' must be greater than 0"
        }
        require(!(internalTopicsRf < 0 || internalTopicsRf > brokersCount)) {
            "internalTopicsRf '$internalTopicsRf' must be less than 'brokersCount' and greater than '0'"
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
                    .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicsRf.toString() + "")
                    .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicsRf.toString() + "")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicsRf.toString() + "")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", internalTopicsRf.toString() + "")
                    .withStartupTimeout(Duration.ofMinutes(1))
            )
        }
    }

    fun brokers(): List<KafkaContainer> = brokers

    fun bootstrapServers(): String =
        brokers.joinToString { kafkaContainer: KafkaContainer -> kafkaContainer.bootstrapServers }

    override fun start() {
        brokers.forEach(KafkaContainer::start)
        brokers.forEach { container ->
            await withPollInterval Duration.ofMillis(500) atMost Duration.ofSeconds(30) untilAsserted {
                container.isRunning
            }
        }
//        await withPollInterval Duration.ofMillis(500) atMost Duration.ofSeconds(30) untilAsserted {
//            val execResult: Container.ExecResult? = zookeeper?.execInContainer(
//                "sh",
//                "-c",
//                "zookeeper-shell zookeeper:" + KafkaContainer.ZOOKEEPER_PORT + " ls /brokers/ids | tail -n 1"
//            )
//            val brokers = execResult?.stdout
//            logger.info { "brokers is: $brokers" }
//            brokers?.split(",")?.size shouldBe brokersNum
//        }
    }

    override fun stop() {
        brokers.forEach(KafkaContainer::stop)
        zookeeper?.stop()
    }
}
