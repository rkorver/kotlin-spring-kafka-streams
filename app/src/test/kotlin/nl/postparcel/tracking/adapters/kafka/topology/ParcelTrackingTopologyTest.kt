package nl.postparcel.tracking.adapters.kafka.topology

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import nl.postparcel.tracking.events.v1.DeliveryScan
import nl.postparcel.tracking.events.v1.ParcelJourneyCompleted
import nl.postparcel.tracking.events.v1.ParcelJourneyState
import nl.postparcel.tracking.events.v1.ServicePointScan
import nl.postparcel.tracking.events.v1.SortingCenterScan
import nl.postparcel.tracking.events.v1.SortingCenterScanType
import nl.postparcel.tracking.fixtures.ParcelEventFixtures.delivered
import nl.postparcel.tracking.fixtures.ParcelEventFixtures.servicePoint
import nl.postparcel.tracking.fixtures.ParcelEventFixtures.sortingCenterScan
import nl.postparcel.tracking.topics.KafkaTopics
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.Properties

class ParcelTrackingTopologyTest {
    private lateinit var testDriver: TopologyTestDriver
    private lateinit var receivedTopic: TestInputTopic<String, ServicePointScan>
    private lateinit var sortingCenterTopic: TestInputTopic<String, SortingCenterScan>
    private lateinit var deliveredTopic: TestInputTopic<String, DeliveryScan>
    private lateinit var completedTopic: TestOutputTopic<String, ParcelJourneyCompleted>

    private val schemaRegistryClient = MockSchemaRegistryClient()
    private val schemaRegistryUrl = "mock://test"

    private inline fun <reified T : SpecificRecord> avroSerde(): SpecificAvroSerde<T> {
        val serde = SpecificAvroSerde<T>(schemaRegistryClient)
        serde.configure(
            mapOf(SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl, SPECIFIC_AVRO_READER_CONFIG to "true"),
            false,
        )
        return serde
    }

    @BeforeEach
    fun setup() {
        val stringSerde = Serdes.String()
        val servicePointScanSerde = avroSerde<ServicePointScan>()
        val sortingCenterScanSerde = avroSerde<SortingCenterScan>()
        val deliveryScanSerde = avroSerde<DeliveryScan>()
        val parcelJourneyStateSerde = avroSerde<ParcelJourneyState>()
        val parcelJourneyCompletedSerde = avroSerde<ParcelJourneyCompleted>()

        val topology =
            ParcelTrackingTopology(
                stringSerde,
                servicePointScanSerde,
                sortingCenterScanSerde,
                deliveryScanSerde,
                parcelJourneyStateSerde,
                parcelJourneyCompletedSerde,
            )

        val builder = StreamsBuilder()
        topology.buildTopology(builder)

        val props =
            Properties().apply {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
                put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
                put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde::class.java.name)
                put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
            }

        testDriver = TopologyTestDriver(builder.build(), props)

        receivedTopic =
            testDriver.createInputTopic(
                KafkaTopics.PARCEL_RECEIVED,
                stringSerde.serializer(),
                servicePointScanSerde.serializer(),
            )
        sortingCenterTopic =
            testDriver.createInputTopic(
                KafkaTopics.SORTING_CENTER_EVENTS,
                stringSerde.serializer(),
                sortingCenterScanSerde.serializer(),
            )
        deliveredTopic =
            testDriver.createInputTopic(
                KafkaTopics.PARCEL_DELIVERED,
                stringSerde.serializer(),
                deliveryScanSerde.serializer(),
            )
        completedTopic =
            testDriver.createOutputTopic(
                KafkaTopics.JOURNEY_COMPLETED,
                stringSerde.deserializer(),
                parcelJourneyCompletedSerde.deserializer(),
            )
    }

    @AfterEach
    fun tearDown() {
        testDriver.close()
    }

    @Test
    fun `complete journey with all three events emits ParcelJourneyCompleted`() {
        val parcelId = "parcel-1"
        val trackingCode = "3SKABA001"
        val now = Instant.parse("2026-04-20T10:00:00Z")

        receivedTopic.pipeInput(parcelId, servicePoint(parcelId, trackingCode, now))
        sortingCenterTopic.pipeInput(
            parcelId,
            sortingCenterScan(parcelId, trackingCode, SortingCenterScanType.READY_FOR_DELIVERY, now.plusSeconds(3600)),
        )
        deliveredTopic.pipeInput(parcelId, delivered(parcelId, trackingCode, now.plusSeconds(7200)))

        val results = completedTopic.readValuesToList()
        assertThat(results).hasSize(1)
        assertThat(results[0].parcelId).isEqualTo(parcelId)
        assertThat(results[0].trackingCode).isEqualTo(trackingCode)
        assertThat(results[0].totalDurationMillis).isEqualTo(7200_000L)
    }

    @Test
    fun `missing READY_FOR_DELIVERY does not emit`() {
        val parcelId = "parcel-2"
        val trackingCode = "3SKABA002"
        val now = Instant.now()

        receivedTopic.pipeInput(parcelId, servicePoint(parcelId, trackingCode, now))
        deliveredTopic.pipeInput(parcelId, delivered(parcelId, trackingCode, now.plusSeconds(100)))

        assertThat(completedTopic.isEmpty).isTrue()
    }

    @Test
    fun `non-READY_FOR_DELIVERY sorting center events are filtered out`() {
        val parcelId = "parcel-3"
        val trackingCode = "3SKABA003"
        val now = Instant.now()

        receivedTopic.pipeInput(parcelId, servicePoint(parcelId, trackingCode, now))
        sortingCenterTopic.pipeInput(
            parcelId,
            sortingCenterScan(parcelId, trackingCode, SortingCenterScanType.ARRIVED, now.plusSeconds(60)),
        )
        sortingCenterTopic.pipeInput(
            parcelId,
            sortingCenterScan(parcelId, trackingCode, SortingCenterScanType.EXCEPTION, now.plusSeconds(120)),
        )
        deliveredTopic.pipeInput(parcelId, delivered(parcelId, trackingCode, now.plusSeconds(180)))

        assertThat(completedTopic.isEmpty).isTrue()
    }

    @Test
    fun `out-of-order events still produce completed journey`() {
        val parcelId = "parcel-4"
        val trackingCode = "3SKABA004"
        val now = Instant.parse("2026-04-20T10:00:00Z")

        // Deliver first, then sorting center, then service point
        deliveredTopic.pipeInput(parcelId, delivered(parcelId, trackingCode, now.plusSeconds(7200)))
        sortingCenterTopic.pipeInput(
            parcelId,
            sortingCenterScan(parcelId, trackingCode, SortingCenterScanType.READY_FOR_DELIVERY, now.plusSeconds(3600)),
        )
        receivedTopic.pipeInput(parcelId, servicePoint(parcelId, trackingCode, now))

        val results = completedTopic.readValuesToList()
        assertThat(results).hasSize(1)
        assertThat(results[0].parcelId).isEqualTo(parcelId)
    }

    @Test
    fun `late READY_FOR_DELIVERY completes a previously incomplete journey`() {
        val parcelId = "parcel-5"
        val trackingCode = "3SKABA005"
        val now = Instant.now()

        receivedTopic.pipeInput(parcelId, servicePoint(parcelId, trackingCode, now))
        deliveredTopic.pipeInput(parcelId, delivered(parcelId, trackingCode, now.plusSeconds(180)))

        assertThat(completedTopic.isEmpty).isTrue()

        sortingCenterTopic.pipeInput(
            parcelId,
            sortingCenterScan(parcelId, trackingCode, SortingCenterScanType.READY_FOR_DELIVERY, now.plusSeconds(240)),
        )

        val results = completedTopic.readValuesToList()
        assertThat(results).hasSize(1)
    }

    @Test
    fun `duplicate events after completion do not re-emit`() {
        val parcelId = "parcel-6"
        val trackingCode = "3SKABA006"
        val now = Instant.parse("2026-04-20T10:00:00Z")

        receivedTopic.pipeInput(parcelId, servicePoint(parcelId, trackingCode, now))
        sortingCenterTopic.pipeInput(
            parcelId,
            sortingCenterScan(parcelId, trackingCode, SortingCenterScanType.READY_FOR_DELIVERY, now.plusSeconds(3600)),
        )
        deliveredTopic.pipeInput(parcelId, delivered(parcelId, trackingCode, now.plusSeconds(7200)))

        assertThat(completedTopic.readValuesToList()).hasSize(1)

        // Send duplicate delivery
        deliveredTopic.pipeInput(parcelId, delivered(parcelId, trackingCode, now.plusSeconds(7200)))

        // No duplicate emission
        assertThat(completedTopic.isEmpty).isTrue()
    }
}
