package nl.postparcel.tracking.domain.port.outbound

import nl.postparcel.tracking.domain.model.CompletedParcelJourney
import nl.postparcel.tracking.domain.model.ParcelId

interface ParcelJourneyRepository {
    fun save(journey: CompletedParcelJourney): CompletedParcelJourney

    fun findByParcelId(parcelId: ParcelId): CompletedParcelJourney?
}
