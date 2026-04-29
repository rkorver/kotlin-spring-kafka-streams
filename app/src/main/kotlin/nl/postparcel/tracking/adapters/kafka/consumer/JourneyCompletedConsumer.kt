package nl.postparcel.tracking.adapters.kafka.consumer

import nl.postparcel.tracking.adapters.kafka.mapper.CompletedJourneyMapper.toDomain
import nl.postparcel.tracking.domain.service.ParcelJourneyService
import nl.postparcel.tracking.events.v1.ParcelJourneyCompleted
import nl.postparcel.tracking.logger
import nl.postparcel.tracking.topics.KafkaTopics.JOURNEY_COMPLETED
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class JourneyCompletedConsumer(
    private val persistenceService: ParcelJourneyService,
) {
    @KafkaListener(
        topics = [JOURNEY_COMPLETED],
        groupId = "\${spring.application.name:parcel-tracking}-persistence",
        containerFactory = "avroKafkaListenerContainerFactory",
    )
    fun onJourneyCompleted(event: ParcelJourneyCompleted) {
        logger.info("Received ParcelJourneyCompleted parcelId={} trackingCode={}", event.parcelId, event.trackingCode)

        event
            .toDomain()
            .let(persistenceService::persist)
    }
}
