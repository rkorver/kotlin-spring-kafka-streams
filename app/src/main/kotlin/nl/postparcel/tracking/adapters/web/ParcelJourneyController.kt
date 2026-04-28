package nl.postparcel.tracking.adapters.web

import nl.postparcel.tracking.api.ParcelJourneyApi
import nl.postparcel.tracking.api.ParcelJourneyResponse
import nl.postparcel.tracking.domain.model.CompletedParcelJourney
import nl.postparcel.tracking.domain.model.ParcelId
import nl.postparcel.tracking.domain.service.ParcelJourneyService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RestController

@RestController
class ParcelJourneyController(
    private val parcelJourneyService: ParcelJourneyService,
) : ParcelJourneyApi {
    override fun getJourney(parcelId: String): ResponseEntity<ParcelJourneyResponse> =
        ParcelId(parcelId)
            .let(parcelJourneyService::getParcelJourneyById)
            ?.toResponse()
            ?.let { ResponseEntity.ok(it) }
            ?: ResponseEntity.notFound().build()
}

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
