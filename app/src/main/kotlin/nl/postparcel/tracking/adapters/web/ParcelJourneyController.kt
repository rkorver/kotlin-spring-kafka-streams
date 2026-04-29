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
    val postalOfficeCity: String,
    val receivedAt: String,
    val sortingCenterCity: String,
    val readyForDeliveryAt: String,
    val deliveryCity: String,
    val deliveredAt: String,
    val receivedBy: String,
    val totalDurationMillis: Long,
)

private fun CompletedParcelJourney.toResponse(): ParcelJourneyResponse =
    ParcelJourneyResponse(
        parcelId = parcelId.value,
        trackingCode = trackingCode.value,
        postalOfficeCity = postalOffice.postalOfficeCity,
        receivedAt = postalOffice.scannedAt.toString(),
        sortingCenterCity = readyForDelivery.sortingCenterCity,
        readyForDeliveryAt = readyForDelivery.scannedAt.toString(),
        deliveryCity = delivered.deliveryAddress.city,
        deliveredAt = delivered.deliveredAt.toString(),
        receivedBy = delivered.receivedBy.name,
        totalDurationMillis = totalDuration.toMillis(),
    )
