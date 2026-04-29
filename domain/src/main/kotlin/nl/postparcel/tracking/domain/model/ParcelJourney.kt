package nl.postparcel.tracking.domain.model

import java.time.Duration

data class ParcelJourney(
    val parcelId: ParcelId,
    val trackingCode: TrackingCode?,
    val servicePoint: ServicePointEvent?,
    val readyForDelivery: ReadyForDeliveryEvent?,
    val delivered: DeliveryEvent?,
) {
    fun withServicePoint(
        event: ServicePointEvent,
        trackingCode: TrackingCode,
    ): ParcelJourney = copy(servicePoint = event, trackingCode = this.trackingCode ?: trackingCode)

    fun withReadyForDelivery(
        event: ReadyForDeliveryEvent,
        trackingCode: TrackingCode,
    ): ParcelJourney = copy(readyForDelivery = event, trackingCode = this.trackingCode ?: trackingCode)

    fun withDelivered(
        event: DeliveryEvent,
        trackingCode: TrackingCode,
    ): ParcelJourney = copy(delivered = event, trackingCode = this.trackingCode ?: trackingCode)

    fun isComplete(): Boolean = servicePoint != null && readyForDelivery != null && delivered != null

    fun toCompleted(): CompletedParcelJourney {
        check(isComplete()) { "Journey $parcelId is not complete yet" }
        val code = requireNotNull(trackingCode) { "trackingCode is required to complete a journey" }
        return CompletedParcelJourney(
            parcelId = parcelId,
            trackingCode = code,
            servicePoint = servicePoint!!,
            readyForDelivery = readyForDelivery!!,
            delivered = delivered!!,
            totalDuration = Duration.between(servicePoint.scannedAt, delivered.scannedAt),
        )
    }

    companion object {
        fun empty(parcelId: ParcelId): ParcelJourney =
            ParcelJourney(
                parcelId = parcelId,
                trackingCode = null,
                servicePoint = null,
                readyForDelivery = null,
                delivered = null,
            )
    }
}

data class CompletedParcelJourney(
    val parcelId: ParcelId,
    val trackingCode: TrackingCode,
    val servicePoint: ServicePointEvent,
    val readyForDelivery: ReadyForDeliveryEvent,
    val delivered: DeliveryEvent,
    val totalDuration: Duration,
)
