package nl.postparcel.tracking.domain.port.inbound

import nl.postparcel.tracking.domain.model.CompletedParcelJourney
import nl.postparcel.tracking.domain.model.ParcelId

interface ParcelJourneyPort {
    fun getParcelJourneyById(id: ParcelId): CompletedParcelJourney?
}
