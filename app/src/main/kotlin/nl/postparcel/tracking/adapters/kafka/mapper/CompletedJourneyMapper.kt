package nl.postparcel.tracking.adapters.kafka.mapper

import nl.postparcel.tracking.adapters.kafka.mapper.AvroDomainMappers.toDeliveryScan
import nl.postparcel.tracking.adapters.kafka.mapper.AvroDomainMappers.toPostalOfficeScan
import nl.postparcel.tracking.adapters.kafka.mapper.AvroDomainMappers.toReadyForDeliveryScan
import nl.postparcel.tracking.domain.model.CompletedParcelJourney
import nl.postparcel.tracking.domain.model.ParcelId
import nl.postparcel.tracking.domain.model.TrackingCode
import nl.postparcel.tracking.events.v1.ParcelJourneyCompleted
import java.time.Duration

object CompletedJourneyMapper {
    fun ParcelJourneyCompleted.toDomain(): CompletedParcelJourney =
        CompletedParcelJourney(
            parcelId = ParcelId(parcelId),
            trackingCode = TrackingCode(trackingCode),
            postalOffice = postalOffice.toPostalOfficeScan(),
            readyForDelivery = readyForDelivery.toReadyForDeliveryScan(),
            delivered = delivered.toDeliveryScan(),
            totalDuration = Duration.ofMillis(totalDurationMillis),
        )
}
