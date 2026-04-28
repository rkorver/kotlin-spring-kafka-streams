package nl.postparcel.tracking.domain.service

import nl.postparcel.tracking.domain.model.CompletedParcelJourney
import nl.postparcel.tracking.domain.model.ParcelId
import nl.postparcel.tracking.domain.port.ParcelJourneyRepository
import nl.postparcel.tracking.logger
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class ParcelJourneyService(
    private val repository: ParcelJourneyRepository,
) {
    fun getParcelJourneyById(id: ParcelId) = repository.findByParcelId(id)

    fun persist(completedParcelJourney: CompletedParcelJourney) {
        repository.save(completedParcelJourney)
        logger.info("Persisted completed journey parcelId={}", completedParcelJourney.parcelId)
    }
}
