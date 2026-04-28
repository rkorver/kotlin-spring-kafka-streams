package nl.postparcel.tracking.api

data class ParcelJourneyResponse(
    val parcelId: String,
    val trackingCode: String,
    val postalOfficeCity: String,
    val receivedAt: String,
    val sortingCenterCity: String,
    val readyForDeliveryAt: String,
    val deliveryCity: String,
    val deliveredAt: String,
    val receivedBy: String,
    val totalDurationMillis: Long,
)
