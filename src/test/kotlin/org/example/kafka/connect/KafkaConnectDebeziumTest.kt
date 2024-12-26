package org.example.kafka.connect

import io.debezium.testing.testcontainers.ConnectorConfiguration
import io.debezium.testing.testcontainers.ConnectorConfiguration.CONNECTOR
import io.debezium.testing.testcontainers.DebeziumContainer
import io.kotest.assertions.json.shouldContainJsonKeyValue
import mu.KLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.time.Duration
import java.util.*


@Suppress("DEPRECATION")
class KafkaConnectDebeziumTest {

    companion object : KLogging() {

        private val network: Network = Network.newNetwork()

        private val kafkaContainer: KafkaContainer = KafkaContainer().withNetwork(network)

        private val postgresContainer: PostgreSQLContainer<*> = PostgreSQLContainer(
            DockerImageName.parse("quay.io/debezium/postgres:15")
                .asCompatibleSubstituteFor("postgres")
        ).withNetwork(network)
            .withNetworkAliases("postgres")

        private val debeziumContainer: DebeziumContainer =
            DebeziumContainer("quay.io/debezium/connect:3.0.5.Final")
                .withNetwork(network)
                .withKafka(kafkaContainer)
                .dependsOn(kafkaContainer)


        @BeforeAll
        @JvmStatic
        fun runContainers() {
            kafkaContainer.start()
            postgresContainer.start()
            postgresContainer.also {
                logger.info { "jdbcUrl: ${it.jdbcUrl}" }
                logger.info { "login: ${it.username}" }
                logger.info { "login: ${it.password}" }
            }
            debeziumContainer.start()
        }

        @AfterAll
        @JvmStatic
        fun stopContainers() {
            kafkaContainer.stop()
            postgresContainer.stop()
            debeziumContainer.stop()
        }
    }

    @Test
    fun `should publish to kafka db rows from source table`() {
        val topicPrefix = "postge-connector"
        val topicName = "postge-connector.public.source_table"

        val expectedDbIdField: UUID = UUID.randomUUID()
        val expectedDbDataField: String = "my-test-data"

        val connection: Connection = createConnection(postgresContainer)
        val statement: Statement = connection.createStatement()
        val consumer: KafkaConsumer<String, String> = createConsumer(kafkaContainer)

        statement.execute(
            //language=sql
            """
            create table source_table(
                id   uuid        not null,
                data varchar(30) not null
            )
            """.trimIndent()
        )
        statement.execute(
            //language=sql
            """
            insert into source_table(id, data) values(
            '$expectedDbIdField', '$expectedDbDataField'
            )
            """.trimIndent()
        )

        ConnectorConfiguration.forJdbcContainer(postgresContainer)
            .with(CONNECTOR, "io.debezium.connector.postgresql.PostgresConnector").with("topic.prefix", topicPrefix)
            .apply {
                debeziumContainer.registerConnector("my-connector", this)
            }

        consumer.subscribe(listOf(topicName))

        val records: MutableIterable<ConsumerRecord<String, String>> = consumer.poll(Duration.ofSeconds(5))

        logger.info { "records: ${records.first().key()} ${records.first().value()}" }

        records.first().value().shouldContainJsonKeyValue("$.after.id", "$expectedDbIdField")
        records.first().value().shouldContainJsonKeyValue("$.after.data", expectedDbDataField)

        statement.close()
        connection.close()
        consumer.unsubscribe()
        consumer.close()
    }

    private fun createConnection(postgresContainer: PostgreSQLContainer<*>): Connection =
        DriverManager.getConnection(postgresContainer.jdbcUrl, postgresContainer.username, postgresContainer.password)

    private fun createConsumer(
        kafkaContainer: KafkaContainer
    ): KafkaConsumer<String, String> = KafkaConsumer(
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "tc-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        ),
        StringDeserializer(),
        StringDeserializer()
    )
}
