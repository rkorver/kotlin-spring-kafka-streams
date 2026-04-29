package nl.postparcel.tracking.adapters.web

import nl.postparcel.tracking.domain.model.CompletedParcelJourney
import nl.postparcel.tracking.domain.model.ParcelId
import nl.postparcel.tracking.domain.service.ParcelJourneyService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/parcels")
class ParcelJourneyController(
    private val parcelJourneyService: ParcelJourneyService,
) {
    @GetMapping("/{parcelId}")
    fun getJourney(
        @PathVariable parcelId: String,
    ): ResponseEntity<ParcelJourneyResponse> =
        ParcelId(parcelId)
            .let(parcelJourneyService::getParcelJourneyById)
            ?.toResponse()
            ?.let { ResponseEntity.ok(it) }
            ?: ResponseEntity.notFound().build()
}

data class ParcelJourneyResponse(
    val parcelId: String,
    val trackingCode: String,
    val servicePointId: String,
    val receivedAt: String,
    val sortingCenterId: String,
    val readyForDeliveryAt: String,
    val deliveryCity: String,
    val deliveredAt: String,
    val deliveryType: String,
    val totalDurationMillis: Long,
)

private fun CompletedParcelJourney.toResponse(): ParcelJourneyResponse =
    ParcelJourneyResponse(
        parcelId = parcelId.value,
        trackingCode = trackingCode.value,
        servicePointId = servicePoint.servicePointId,
        receivedAt = servicePoint.scannedAt.toString(),
        sortingCenterId = readyForDelivery.sortingCenterId,
        readyForDeliveryAt = readyForDelivery.scannedAt.toString(),
        deliveryCity = delivered.deliveryAddress.city,
        deliveredAt = delivered.scannedAt.toString(),
        deliveryType = delivered.deliveryType.name,
        totalDurationMillis = totalDuration.toMillis(),
    )
