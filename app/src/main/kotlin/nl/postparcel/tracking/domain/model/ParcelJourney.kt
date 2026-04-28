package nl.postparcel.tracking.domain.model

import java.time.Duration

data class ParcelJourney(
    val parcelId: ParcelId,
    val trackingCode: TrackingCode?,
    val postalOffice: PostalOfficeScan?,
    val readyForDelivery: ReadyForDeliveryScan?,
    val delivered: DeliveryScan?,
) {
    fun withPostalOffice(
        scan: PostalOfficeScan,
        trackingCode: TrackingCode,
    ): ParcelJourney = copy(postalOffice = scan, trackingCode = this.trackingCode ?: trackingCode)

    fun withReadyForDelivery(
        scan: ReadyForDeliveryScan,
        trackingCode: TrackingCode,
    ): ParcelJourney = copy(readyForDelivery = scan, trackingCode = this.trackingCode ?: trackingCode)

    fun withDelivered(
        scan: DeliveryScan,
        trackingCode: TrackingCode,
    ): ParcelJourney = copy(delivered = scan, trackingCode = this.trackingCode ?: trackingCode)

    fun isComplete(): Boolean = postalOffice != null && readyForDelivery != null && delivered != null

    fun toCompleted(): CompletedParcelJourney {
        check(isComplete()) { "Journey $parcelId is not complete yet" }
        val code = requireNotNull(trackingCode) { "trackingCode is required to complete a journey" }
        return CompletedParcelJourney(
            parcelId = parcelId,
            trackingCode = code,
            postalOffice = postalOffice!!,
            readyForDelivery = readyForDelivery!!,
            delivered = delivered!!,
            totalDuration = Duration.between(postalOffice.scannedAt, delivered.deliveredAt),
        )
    }

    companion object {
        fun empty(parcelId: ParcelId): ParcelJourney =
            ParcelJourney(
                parcelId = parcelId,
                trackingCode = null,
                postalOffice = null,
                readyForDelivery = null,
                delivered = null,
            )
    }
}

data class CompletedParcelJourney(
    val parcelId: ParcelId,
    val trackingCode: TrackingCode,
    val postalOffice: PostalOfficeScan,
    val readyForDelivery: ReadyForDeliveryScan,
    val delivered: DeliveryScan,
    val totalDuration: Duration,
)
