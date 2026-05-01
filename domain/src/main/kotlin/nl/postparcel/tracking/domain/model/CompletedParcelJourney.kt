package nl.postparcel.tracking.domain.model

import java.time.Duration

data class CompletedParcelJourney(
    val parcelId: ParcelId,
    val trackingCode: TrackingCode,
    val servicePoint: ServicePointEvent,
    val readyForDelivery: ReadyForDeliveryEvent,
    val delivered: DeliveryEvent,
    val totalDuration: Duration,
)
