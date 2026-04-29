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
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.during
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.Network
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.redpanda.RedpandaContainer
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

        await during Duration.ofSeconds(2) atMost Duration.ofSeconds(3) untilAsserted {
            assertThat(parcelJourneyService.getParcelJourneyById(parcelId)).isNull()
        }
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

        await during Duration.ofSeconds(2) atMost Duration.ofSeconds(3) untilAsserted {
            assertThat(parcelJourneyService.getParcelJourneyById(parcelId)).isNull()
        }

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
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, redpanda.bootstrapServers)
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
        val redpanda: RedpandaContainer =
            RedpandaContainer(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v24.3.1"))
                .withNetwork(NETWORK)
                .withNetworkAliases("redpanda")

        @Container
        @JvmStatic
        val postgres: PostgreSQLContainer =
            PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine"))
                .withDatabaseName("parcel_tracking")
                .withUsername("parcel")
                .withPassword("parcel")

        @JvmStatic
        fun schemaRegistryUrl(): String = redpanda.schemaRegistryAddress

        private fun createTopics() {
            AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to redpanda.bootstrapServers)).use { admin ->
                val topics =
                    listOf(
                        KafkaTopics.PARCEL_RECEIVED,
                        KafkaTopics.SORTING_CENTER_EVENTS,
                        KafkaTopics.PARCEL_DELIVERED,
                        KafkaTopics.JOURNEY_COMPLETED,
                    ).map { NewTopic(it, 3, 1.toShort()) }
                admin.createTopics(topics).all().get()
            }
        }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            createTopics()
            registry.add("spring.kafka.bootstrap-servers") { redpanda.bootstrapServers }
            registry.add("spring.kafka.properties.schema.registry.url") { redpanda.schemaRegistryAddress }
            registry.add("spring.datasource.url") { postgres.jdbcUrl }
            registry.add("spring.datasource.username") { postgres.username }
            registry.add("spring.datasource.password") { postgres.password }
        }
    }
}
