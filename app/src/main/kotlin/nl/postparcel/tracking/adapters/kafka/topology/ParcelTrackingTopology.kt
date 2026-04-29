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
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
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

        val readyForDeliveryStream =
            builder
                .stream(SORTING_CENTER_EVENTS, Consumed.with(stringSerde, sortingCenterScanSerde))
                .filter { _, value -> value.scanType == READY_FOR_DELIVERY }
                .selectKey { _, value -> value.parcelId }
                .mapValues(::partialFromReadyForDelivery)

        val deliveredStream =
            builder
                .stream(PARCEL_DELIVERED, Consumed.with(stringSerde, deliveryScanSerde))
                .selectKey { _, value -> value.parcelId }
                .mapValues(::partialFromDelivered)

        mergeAndAggregateToTopic(
            builder,
            servicePointStream,
            readyForDeliveryStream,
            deliveredStream,
            JOURNEY_COMPLETED,
        )

        return builder
    }

    private fun mergeAndAggregateToTopic(
        builder: StreamsBuilder,
        servicePointStream: KStream<String, ParcelJourneyState>,
        readyForDeliveryStream: KStream<String, ParcelJourneyState>,
        deliveredStream: KStream<String, ParcelJourneyState>,
        topic: String,
    ) {
        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE_NAME),
                stringSerde,
                parcelJourneyStateSerde,
            ),
        )

        val processorSupplier =
            ProcessorSupplier<String, ParcelJourneyState, String, ParcelJourneyCompleted> {
                object : Processor<String, ParcelJourneyState, String, ParcelJourneyCompleted> {
                    private lateinit var context: ProcessorContext<String, ParcelJourneyCompleted>
                    private lateinit var store: KeyValueStore<String, ParcelJourneyState>

                    override fun init(context: ProcessorContext<String, ParcelJourneyCompleted>) {
                        this.context = context
                        @Suppress("UNCHECKED_CAST")
                        store = context.getStateStore(STATE_STORE_NAME) as KeyValueStore<String, ParcelJourneyState>
                    }

                    override fun process(record: Record<String, ParcelJourneyState>) {
                        val key = record.key()
                        val incoming = record.value()
                        val current = store.get(key) ?: emptyState()
                        val merged = mergeState(current, incoming)

                        if (merged.complete) {
                            store.delete(key)
                            context.forward(Record(key, toCompletedEvent(merged), record.timestamp()))
                        } else {
                            store.put(key, merged)
                        }
                    }
                }
            }

        servicePointStream
            .merge(readyForDeliveryStream)
            .merge(deliveredStream)
            .process(processorSupplier, Named.`as`("journey-aggregator"), STATE_STORE_NAME)
            .to(topic, Produced.with(stringSerde, parcelJourneyCompletedSerde))
    }

    private fun emptyState(): ParcelJourneyState =
        ParcelJourneyState
            .newBuilder()
            .setParcelId("")
            .setTrackingCode(null)
            .setServicePoint(null)
            .setReadyForDelivery(null)
            .setDelivered(null)
            .setComplete(false)
            .setAlreadyEmitted(false)
            .build()

    private fun partialFromServicePoint(scan: ServicePointScan): ParcelJourneyState =
        ParcelJourneyState
            .newBuilder()
            .setParcelId(scan.parcelId)
            .setTrackingCode(scan.trackingCode)
            .setServicePoint(scan)
            .setReadyForDelivery(null)
            .setDelivered(null)
            .setComplete(false)
            .setAlreadyEmitted(false)
            .build()

    private fun partialFromReadyForDelivery(scan: SortingCenterScan): ParcelJourneyState =
        ParcelJourneyState
            .newBuilder()
            .setParcelId(scan.parcelId)
            .setTrackingCode(scan.trackingCode)
            .setServicePoint(null)
            .setReadyForDelivery(scan)
            .setDelivered(null)
            .setComplete(false)
            .setAlreadyEmitted(false)
            .build()

    private fun partialFromDelivered(scan: DeliveryScan): ParcelJourneyState =
        ParcelJourneyState
            .newBuilder()
            .setParcelId(scan.parcelId)
            .setTrackingCode(scan.trackingCode)
            .setServicePoint(null)
            .setReadyForDelivery(null)
            .setDelivered(scan)
            .setComplete(false)
            .setAlreadyEmitted(false)
            .build()

    private fun mergeState(
        current: ParcelJourneyState,
        incoming: ParcelJourneyState,
    ): ParcelJourneyState {
        val parcelId = listOf(current.parcelId, incoming.parcelId).firstOrNull { !it.isNullOrBlank() }.orEmpty()
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
