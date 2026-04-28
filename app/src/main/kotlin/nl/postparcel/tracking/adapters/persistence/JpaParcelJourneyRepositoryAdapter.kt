package nl.postparcel.tracking.adapters.persistence

import nl.postparcel.tracking.domain.model.Address
import nl.postparcel.tracking.domain.model.CompletedParcelJourney
import nl.postparcel.tracking.domain.model.DeliveryScan
import nl.postparcel.tracking.domain.model.ParcelId
import nl.postparcel.tracking.domain.model.PostalOfficeScan
import nl.postparcel.tracking.domain.model.ReadyForDeliveryScan
import nl.postparcel.tracking.domain.model.TrackingCode
import nl.postparcel.tracking.domain.port.ParcelJourneyRepository
import org.springframework.stereotype.Component
import java.time.Duration
import kotlin.jvm.optionals.getOrNull

@Component
class JpaParcelJourneyRepositoryAdapter(
    private val jpa: ParcelJourneyJpaRepository,
) : ParcelJourneyRepository {
    override fun save(journey: CompletedParcelJourney) = jpa.save(journey.toEntity()).toDomain()

    override fun findByParcelId(parcelId: ParcelId) = jpa.findById(parcelId.value).getOrNull()?.toDomain()
}

private fun CompletedParcelJourney.toEntity() =
    ParcelJourneyEntity(
        parcelId = parcelId.value,
        trackingCode = trackingCode.value,
        postalOfficeId = postalOffice.postalOfficeId,
        postalOfficeCity = postalOffice.postalOfficeCity,
        receivedAt = postalOffice.scannedAt,
        sortingCenterId = readyForDelivery.sortingCenterId,
        sortingCenterCity = readyForDelivery.sortingCenterCity,
        readyForDeliveryAt = readyForDelivery.scannedAt,
        deliveryCity = delivered.deliveryAddress.city,
        deliveryPostalCode = delivered.deliveryAddress.postalCode,
        deliveredAt = delivered.deliveredAt,
        receivedBy = delivered.receivedBy,
        courierId = delivered.courierId,
        totalDurationMillis = totalDuration.toMillis(),
    )

private fun ParcelJourneyEntity.toDomain(): CompletedParcelJourney =
    CompletedParcelJourney(
        parcelId = ParcelId(parcelId),
        trackingCode = TrackingCode(trackingCode),
        postalOffice =
            PostalOfficeScan(
                postalOfficeId = postalOfficeId,
                postalOfficeCity = postalOfficeCity,
                sender = Address("", "", "", "", "", "NL"),
                recipient = Address("", "", "", "", deliveryCity, "NL"),
                weightGrams = 0,
                scannedAt = receivedAt,
                employeeId = null,
            ),
        readyForDelivery =
            ReadyForDeliveryScan(
                sortingCenterId = sortingCenterId,
                sortingCenterCity = sortingCenterCity,
                destinationHub = null,
                belt = null,
                scannedAt = readyForDeliveryAt,
            ),
        delivered =
            DeliveryScan(
                deliveredAt = deliveredAt,
                deliveryAddress = Address("", "", "", deliveryPostalCode, deliveryCity, "NL"),
                receivedBy = receivedBy,
                courierId = courierId,
                signatureBase64 = null,
                photoEvidenceUrl = null,
            ),
        totalDuration = Duration.ofMillis(totalDurationMillis),
    )
