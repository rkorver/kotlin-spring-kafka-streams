package nl.postparcel.tracking.domain.model

import java.time.Instant

enum class DeliveryType {
    TO_DOOR,
    NEIGHBOUR,
    MAILBOX,
    PICKUP_POINT,
}

data class ServicePointEvent(
    val servicePointId: String,
    val sender: Address,
    val recipient: Address,
    val weightGrams: Int,
    val scannedAt: Instant,
    val employeeId: String?,
)

data class ReadyForDeliveryEvent(
    val sortingCenterId: String,
    val destinationHub: String?,
    val belt: String?,
    val scannedAt: Instant,
)

data class DeliveryEvent(
    val scannedAt: Instant,
    val deliveryAddress: Address,
    val deliveryType: DeliveryType,
    val courierId: String,
    val signatureBase64: String?,
    val photoEvidenceUrl: String?,
)
