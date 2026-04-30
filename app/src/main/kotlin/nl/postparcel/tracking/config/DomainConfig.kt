package nl.postparcel.tracking.config

import nl.postparcel.tracking.domain.port.outbound.ParcelJourneyRepository
import nl.postparcel.tracking.domain.service.ParcelJourneyService
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class DomainConfig {
    @Bean
    fun parcelJourneyService(repository: ParcelJourneyRepository) = ParcelJourneyService(repository)
}
