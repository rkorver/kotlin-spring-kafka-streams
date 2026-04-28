package nl.postparcel.tracking.adapters.persistence

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface ParcelJourneyJpaRepository : JpaRepository<ParcelJourneyEntity, String>
