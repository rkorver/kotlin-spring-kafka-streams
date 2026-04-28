package nl.postparcel.tracking.domain.model

import java.time.Instant

enum class ReceiverType {
    CUSTOMER,
    NEIGHBOUR,
    MAILBOX,
    PICKUP_POINT,
}

data class PostalOfficeScan(
    val postalOfficeId: String,
    val postalOfficeCity: String,
    val sender: Address,
    val recipient: Address,
    val weightGrams: Int,
    val scannedAt: Instant,
    val employeeId: String?,
)

data class ReadyForDeliveryScan(
    val sortingCenterId: String,
    val sortingCenterCity: String,
    val destinationHub: String?,
    val belt: String?,
    val scannedAt: Instant,
)

data class DeliveryScan(
    val deliveredAt: Instant,
    val deliveryAddress: Address,
    val receivedBy: ReceiverType,
    val courierId: String,
    val signatureBase64: String?,
    val photoEvidenceUrl: String?,
)
