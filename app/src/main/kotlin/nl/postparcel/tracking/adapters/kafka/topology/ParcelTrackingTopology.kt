package nl.postparcel.tracking.adapters.kafka.topology

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import nl.postparcel.tracking.events.v1.ParcelDeliveredToCustomer
import nl.postparcel.tracking.events.v1.ParcelJourneyCompleted
import nl.postparcel.tracking.events.v1.ParcelJourneyState
import nl.postparcel.tracking.events.v1.ParcelReceivedAtPostalOffice
import nl.postparcel.tracking.events.v1.SortingCenterEvent
import nl.postparcel.tracking.events.v1.SortingCenterEventType.READY_FOR_DELIVERY
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
    private val parcelReceivedSerde: SpecificAvroSerde<ParcelReceivedAtPostalOffice>,
    private val sortingCenterEventSerde: SpecificAvroSerde<SortingCenterEvent>,
    private val parcelDeliveredSerde: SpecificAvroSerde<ParcelDeliveredToCustomer>,
    private val parcelJourneyStateSerde: SpecificAvroSerde<ParcelJourneyState>,
    private val parcelJourneyCompletedSerde: SpecificAvroSerde<ParcelJourneyCompleted>,
) {
    fun buildTopology(builder: StreamsBuilder): StreamsBuilder {
        val postalOfficeStream =
            builder
                .stream(PARCEL_RECEIVED, Consumed.with(stringSerde, parcelReceivedSerde))
                .selectKey { _, value -> value.parcelId }
                .mapValues(::partialFromPostalOffice)

        val readyForDeliveryStream =
            builder
                .stream(SORTING_CENTER_EVENTS, Consumed.with(stringSerde, sortingCenterEventSerde))
                .filter { _, value -> value.eventType == READY_FOR_DELIVERY }
                .selectKey { _, value -> value.parcelId }
                .mapValues(::partialFromReadyForDelivery)

        val deliveredStream =
            builder
                .stream(PARCEL_DELIVERED, Consumed.with(stringSerde, parcelDeliveredSerde))
                .selectKey { _, value -> value.parcelId }
                .mapValues(::partialFromDelivered)

        mergeAndAggregateToTopic(postalOfficeStream, readyForDeliveryStream, deliveredStream)(JOURNEY_COMPLETED)

        return builder
    }

    private fun mergeAndAggregateToTopic(
        a: KStream<String, ParcelJourneyState>,
        b: KStream<String, ParcelJourneyState>,
        c: KStream<String, ParcelJourneyState>,
    ) = { topic: String ->
        a
            .merge(b)
            .merge(c)
            .groupByKey(Grouped.with(stringSerde, parcelJourneyStateSerde))
            .aggregate(
                { emptyState() },
                { _, incoming, current -> mergeState(current, incoming) },
                Materialized
                    .`as`<String, ParcelJourneyState, KeyValueStore<Bytes, ByteArray>>(STATE_STORE_NAME)
                    .withKeySerde(stringSerde)
                    .withValueSerde(parcelJourneyStateSerde),
            ).toStream()
            .filter { _, state -> state != null && state.complete && !state.alreadyEmitted }
            .mapValues(::toCompletedEvent)
            .to(topic, Produced.with(stringSerde, parcelJourneyCompletedSerde))
    }

    private fun emptyState(): ParcelJourneyState =
        ParcelJourneyState
            .newBuilder()
            .setParcelId("")
            .setTrackingCode(null)
            .setPostalOffice(null)
            .setReadyForDelivery(null)
            .setDelivered(null)
            .setComplete(false)
            .setAlreadyEmitted(false)
            .build()

    private fun partialFromPostalOffice(event: ParcelReceivedAtPostalOffice): ParcelJourneyState =
        ParcelJourneyState
            .newBuilder()
            .setParcelId(event.parcelId)
            .setTrackingCode(event.trackingCode)
            .setPostalOffice(event)
            .setReadyForDelivery(null)
            .setDelivered(null)
            .setComplete(false)
            .setAlreadyEmitted(false)
            .build()

    private fun partialFromReadyForDelivery(event: SortingCenterEvent): ParcelJourneyState =
        ParcelJourneyState
            .newBuilder()
            .setParcelId(event.parcelId)
            .setTrackingCode(event.trackingCode)
            .setPostalOffice(null)
            .setReadyForDelivery(event)
            .setDelivered(null)
            .setComplete(false)
            .setAlreadyEmitted(false)
            .build()

    private fun partialFromDelivered(event: ParcelDeliveredToCustomer): ParcelJourneyState =
        ParcelJourneyState
            .newBuilder()
            .setParcelId(event.parcelId)
            .setTrackingCode(event.trackingCode)
            .setPostalOffice(null)
            .setReadyForDelivery(null)
            .setDelivered(event)
            .setComplete(false)
            .setAlreadyEmitted(false)
            .build()

    private fun mergeState(
        current: ParcelJourneyState,
        incoming: ParcelJourneyState,
    ): ParcelJourneyState {
        val parcelId = listOf(current.parcelId, incoming.parcelId).firstOrNull { !it.isNullOrBlank() }.orEmpty()
        val trackingCode = current.trackingCode ?: incoming.trackingCode
        val postalOffice = current.postalOffice ?: incoming.postalOffice
        val readyForDelivery = current.readyForDelivery ?: incoming.readyForDelivery
        val delivered = current.delivered ?: incoming.delivered
        val wasComplete = current.complete
        val nowComplete = postalOffice != null && readyForDelivery != null && delivered != null
        val alreadyEmitted = current.alreadyEmitted || wasComplete

        return ParcelJourneyState
            .newBuilder()
            .setParcelId(parcelId)
            .setTrackingCode(trackingCode)
            .setPostalOffice(postalOffice)
            .setReadyForDelivery(readyForDelivery)
            .setDelivered(delivered)
            .setComplete(nowComplete)
            .setAlreadyEmitted(alreadyEmitted)
            .build()
    }

    private fun toCompletedEvent(state: ParcelJourneyState): ParcelJourneyCompleted {
        val postal = state.postalOffice!!
        val ready = state.readyForDelivery!!
        val delivered = state.delivered!!
        return ParcelJourneyCompleted
            .newBuilder()
            .setParcelId(state.parcelId)
            .setTrackingCode(state.trackingCode)
            .setPostalOffice(postal)
            .setReadyForDelivery(ready)
            .setDelivered(delivered)
            .setTotalDurationMillis(Duration.between(postal.scannedAt, delivered.deliveredAt).toMillis())
            .build()
    }

    companion object {
        const val STATE_STORE_NAME = "parcel-journey-state"
    }
}
