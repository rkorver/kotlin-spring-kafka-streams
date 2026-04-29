package nl.postparcel.tracking.adapters.kafka.topology

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import nl.postparcel.tracking.events.v1.DeliveryScan
import nl.postparcel.tracking.events.v1.ParcelJourneyCompleted
import nl.postparcel.tracking.events.v1.ParcelJourneyState
import nl.postparcel.tracking.events.v1.ServicePointScan
import nl.postparcel.tracking.events.v1.SortingCenterScan
import nl.postparcel.tracking.events.v1.SortingCenterScanType.READY_FOR_DELIVERY
import nl.postparcel.tracking.topics.KafkaTopics.JOURNEY_COMPLETED
import nl.postparcel.tracking.topics.KafkaTopics.PARCEL_DELIVERED
import nl.postparcel.tracking.topics.KafkaTopics.PARCEL_RECEIVED
import nl.postparcel.tracking.topics.KafkaTopics.SORTING_CENTER_EVENTS
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class ParcelTrackingTopology(
    private val stringSerde: Serde<String>,
    private val servicePointScanSerde: SpecificAvroSerde<ServicePointScan>,
    private val sortingCenterScanSerde: SpecificAvroSerde<SortingCenterScan>,
    private val deliveryScanSerde: SpecificAvroSerde<DeliveryScan>,
    private val parcelJourneyStateSerde: SpecificAvroSerde<ParcelJourneyState>,
    private val parcelJourneyCompletedSerde: SpecificAvroSerde<ParcelJourneyCompleted>,
) {
    fun buildTopology(builder: StreamsBuilder): StreamsBuilder {
        val servicePointStream =
            builder
                .stream(PARCEL_RECEIVED, Consumed.with(stringSerde, servicePointScanSerde))
                .selectKey { _, value -> value.parcelId }
                .mapValues(::partialFromServicePoint)

        val sortingCenterStream =
            builder
                .stream(SORTING_CENTER_EVENTS, Consumed.with(stringSerde, sortingCenterScanSerde))
                .filter { _, value -> value.scanType == READY_FOR_DELIVERY }
                .selectKey { _, value -> value.parcelId }
                .mapValues(::partialFromSortingCenter)

        val deliveredStream =
            builder
                .stream(PARCEL_DELIVERED, Consumed.with(stringSerde, deliveryScanSerde))
                .selectKey { _, value -> value.parcelId }
                .mapValues(::partialFromDelivered)

        mergeAndAggregateToTopic(
            servicePointStream,
            sortingCenterStream,
            deliveredStream,
            JOURNEY_COMPLETED,
        )

        return builder
    }

    private fun mergeAndAggregateToTopic(
        servicePointStream: KStream<String, ParcelJourneyState>,
        sortingCenterStream: KStream<String, ParcelJourneyState>,
        deliveredStream: KStream<String, ParcelJourneyState>,
        topic: String,
    ) {
        servicePointStream
            .merge(sortingCenterStream)
            .merge(deliveredStream)
            .groupByKey(Grouped.with(stringSerde, parcelJourneyStateSerde))
            .aggregate(
                { emptyState() },
                { _, incoming, current -> mergeState(current, incoming) },
                Materialized
                    .`as`<String, ParcelJourneyState, KeyValueStore<Bytes, ByteArray>>(STATE_STORE_NAME)
                    .withKeySerde(stringSerde)
                    .withValueSerde(parcelJourneyStateSerde),
            ).toStream()
            .filter { _, state -> state.complete && !state.alreadyEmitted }
            .mapValues(::toCompletedEvent)
            .to(topic, Produced.with(stringSerde, parcelJourneyCompletedSerde))
    }

    private fun emptyState(): ParcelJourneyState = partialState("", null)

    private fun partialFromServicePoint(scan: ServicePointScan): ParcelJourneyState =
        partialState(scan.parcelId, scan.trackingCode, servicePoint = scan)

    private fun partialFromSortingCenter(scan: SortingCenterScan): ParcelJourneyState =
        partialState(scan.parcelId, scan.trackingCode, readyForDelivery = scan)

    private fun partialFromDelivered(scan: DeliveryScan): ParcelJourneyState =
        partialState(scan.parcelId, scan.trackingCode, delivered = scan)

    private fun partialState(
        parcelId: String,
        trackingCode: String?,
        servicePoint: ServicePointScan? = null,
        readyForDelivery: SortingCenterScan? = null,
        delivered: DeliveryScan? = null,
    ): ParcelJourneyState =
        ParcelJourneyState
            .newBuilder()
            .setParcelId(parcelId)
            .setTrackingCode(trackingCode)
            .setServicePoint(servicePoint)
            .setReadyForDelivery(readyForDelivery)
            .setDelivered(delivered)
            .setComplete(false)
            .setAlreadyEmitted(false)
            .build()

    private fun mergeState(
        current: ParcelJourneyState,
        incoming: ParcelJourneyState,
    ): ParcelJourneyState {
        val parcelId = current.parcelId.takeUnless { it.isNullOrBlank() } ?: incoming.parcelId.orEmpty()
        val trackingCode = current.trackingCode ?: incoming.trackingCode
        val servicePoint = current.servicePoint ?: incoming.servicePoint
        val readyForDelivery = current.readyForDelivery ?: incoming.readyForDelivery
        val delivered = current.delivered ?: incoming.delivered
        val nowComplete = servicePoint != null && readyForDelivery != null && delivered != null

        return ParcelJourneyState
            .newBuilder()
            .setParcelId(parcelId)
            .setTrackingCode(trackingCode)
            .setServicePoint(servicePoint)
            .setReadyForDelivery(readyForDelivery)
            .setDelivered(delivered)
            .setComplete(nowComplete)
            .setAlreadyEmitted(false)
            .build()
    }

    private fun toCompletedEvent(state: ParcelJourneyState): ParcelJourneyCompleted {
        val servicePoint = state.servicePoint!!
        val delivered = state.delivered!!
        return ParcelJourneyCompleted
            .newBuilder()
            .setParcelId(state.parcelId)
            .setTrackingCode(state.trackingCode)
            .setServicePoint(servicePoint)
            .setReadyForDelivery(state.readyForDelivery)
            .setDelivered(delivered)
            .setTotalDurationMillis(Duration.between(servicePoint.scannedAt, delivered.scannedAt).toMillis())
            .build()
    }

    companion object {
        const val STATE_STORE_NAME = "parcel-journey-state"
    }
}
