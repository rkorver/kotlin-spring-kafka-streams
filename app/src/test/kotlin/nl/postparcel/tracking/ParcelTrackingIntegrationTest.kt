package nl.postparcel.tracking

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import nl.postparcel.tracking.domain.model.ParcelId
import nl.postparcel.tracking.domain.model.TrackingCode
import nl.postparcel.tracking.domain.service.ParcelJourneyService
import nl.postparcel.tracking.events.v1.SortingCenterEventType
import nl.postparcel.tracking.fixtures.ParcelEventFixtures.delivered
import nl.postparcel.tracking.fixtures.ParcelEventFixtures.postalOffice
import nl.postparcel.tracking.fixtures.ParcelEventFixtures.sortingCenterEvent
import nl.postparcel.tracking.topics.KafkaTopics
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.UUID

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class ParcelTrackingIntegrationTest {
    @Autowired
    lateinit var parcelJourneyService: ParcelJourneyService

    @Test
    fun `three Avro events produce one persisted journey row`() {
        val parcelId = ParcelId("parcel-${UUID.randomUUID()}")
        val trackingCode = "3SKABA${System.currentTimeMillis()}"
        val receivedAt = Instant.parse("2026-04-20T09:15:00Z")
        val readyAt = Instant.parse("2026-04-21T03:40:00Z")
        val deliveredAt = Instant.parse("2026-04-21T14:22:00Z")

        publish(
            parcelId.value,
            KafkaTopics.PARCEL_RECEIVED to postalOffice(parcelId.value, trackingCode, receivedAt),
            KafkaTopics.SORTING_CENTER_EVENTS to
                sortingCenterEvent(parcelId.value, trackingCode, SortingCenterEventType.ARRIVED, readyAt.minusSeconds(3600)),
            KafkaTopics.SORTING_CENTER_EVENTS to
                sortingCenterEvent(parcelId.value, trackingCode, SortingCenterEventType.READY_FOR_DELIVERY, readyAt),
            KafkaTopics.PARCEL_DELIVERED to delivered(parcelId.value, trackingCode, deliveredAt),
        )

        await atMost Duration.ofSeconds(60) untilAsserted {
            val persisted = parcelJourneyService.getParcelJourneyById(parcelId)
            assertThat(persisted).isNotNull
            assertThat(persisted?.trackingCode).isEqualTo(TrackingCode(trackingCode))
            assertThat(persisted?.postalOffice?.postalOfficeCity).isEqualTo("Amsterdam")
        }
    }

    @Test
    fun `two events without READY_FOR_DELIVERY do not persist a journey`() {
        val parcelId = ParcelId("parcel-${UUID.randomUUID()}")
        val trackingCode = "3SKABA${System.currentTimeMillis()}"
        val now = Instant.now()

        publish(
            parcelId.value,
            KafkaTopics.PARCEL_RECEIVED to postalOffice(parcelId.value, trackingCode, now),
            KafkaTopics.PARCEL_DELIVERED to delivered(parcelId.value, trackingCode, now.plusSeconds(180)),
        )

        Thread.sleep(5_000)
        assertThat(parcelJourneyService.getParcelJourneyById(parcelId)).isNull()
    }

    @Test
    fun `incomplete journey is not persisted, then recovers when READY_FOR_DELIVERY arrives`() {
        val parcelId = ParcelId("parcel-${UUID.randomUUID()}")
        val trackingCode = "3SKABA${System.currentTimeMillis()}"
        val now = Instant.now()

        publish(
            parcelId.value,
            KafkaTopics.PARCEL_RECEIVED to postalOffice(parcelId.value, trackingCode, now),
            KafkaTopics.SORTING_CENTER_EVENTS to
                sortingCenterEvent(parcelId.value, trackingCode, SortingCenterEventType.ARRIVED, now.plusSeconds(60)),
            KafkaTopics.SORTING_CENTER_EVENTS to
                sortingCenterEvent(parcelId.value, trackingCode, SortingCenterEventType.EXCEPTION, now.plusSeconds(120)),
            KafkaTopics.PARCEL_DELIVERED to delivered(parcelId.value, trackingCode, now.plusSeconds(180)),
        )

        Thread.sleep(5_000)
        assertThat(parcelJourneyService.getParcelJourneyById(parcelId)).isNull()

        publish(
            parcelId.value,
            KafkaTopics.SORTING_CENTER_EVENTS to
                sortingCenterEvent(parcelId.value, trackingCode, SortingCenterEventType.READY_FOR_DELIVERY, now.plusSeconds(240)),
        )

        await atMost Duration.ofSeconds(60) untilAsserted {
            val persisted = parcelJourneyService.getParcelJourneyById(parcelId)
            assertThat(persisted).isNotNull
            assertThat(persisted?.trackingCode).isEqualTo(TrackingCode(trackingCode))
        }
    }

    private fun publish(
        key: String,
        vararg events: Pair<String, SpecificRecord>,
    ) {
        producer().use { producer ->
            events.forEach { (topic, value) -> producer.send(ProducerRecord(topic, key, value)).get() }
            producer.flush()
        }
    }

    private fun producer(): KafkaProducer<String, SpecificRecord> {
        val props =
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
                put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl())
                put(ProducerConfig.ACKS_CONFIG, "all")
            }
        return KafkaProducer(props)
    }

    companion object {
        private val NETWORK: Network = Network.newNetwork()

        @Container
        @JvmStatic
        val kafka: ConfluentKafkaContainer =
            ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                .withNetwork(NETWORK)
                .withNetworkAliases("kafka")
                .withListener("kafka:19092")

        @Container
        @JvmStatic
        val schemaRegistry: GenericContainer<*> =
            GenericContainer(DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1"))
                .withNetwork(NETWORK)
                .withNetworkAliases("schema-registry")
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                .dependsOn(kafka)

        @Container
        @JvmStatic
        val postgres: PostgreSQLContainer =
            PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
                .withDatabaseName("parcel_tracking")
                .withUsername("parcel")
                .withPassword("parcel")

        @JvmStatic
        fun schemaRegistryUrl(): String = "http://${schemaRegistry.host}:${schemaRegistry.getMappedPort(8081)}"

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
            registry.add("spring.kafka.properties.schema.registry.url") { schemaRegistryUrl() }
            registry.add("spring.datasource.url") { postgres.jdbcUrl }
            registry.add("spring.datasource.username") { postgres.username }
            registry.add("spring.datasource.password") { postgres.password }
        }
    }
}
