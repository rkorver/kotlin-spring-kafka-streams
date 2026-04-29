package nl.postparcel.tracking.domain.service

import nl.postparcel.tracking.domain.model.CompletedParcelJourney
import nl.postparcel.tracking.domain.model.ParcelId
import nl.postparcel.tracking.domain.port.ParcelJourneyRepository
import nl.postparcel.tracking.domain.port.inbound.ParcelJourneyPort
import nl.postparcel.tracking.logger

class ParcelJourneyService(
    private val repository: ParcelJourneyRepository,
) : ParcelJourneyPort {
    override fun getParcelJourneyById(id: ParcelId) = repository.findByParcelId(id)

    fun persist(completedParcelJourney: CompletedParcelJourney) {
        repository.save(completedParcelJourney)
        logger.info("Persisted completed journey parcelId={}", completedParcelJourney.parcelId)
    }
}
