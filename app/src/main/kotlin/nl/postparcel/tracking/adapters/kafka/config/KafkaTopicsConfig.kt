package nl.postparcel.tracking.adapters.kafka.config

import nl.postparcel.tracking.topics.KafkaTopics.JOURNEY_COMPLETED
import nl.postparcel.tracking.topics.KafkaTopics.PARCEL_DELIVERED
import nl.postparcel.tracking.topics.KafkaTopics.PARCEL_RECEIVED
import nl.postparcel.tracking.topics.KafkaTopics.SORTING_CENTER_EVENTS
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicsConfig {
    @Bean
    fun parcelReceivedTopic(): NewTopic =
        TopicBuilder
            .name(PARCEL_RECEIVED)
            .partitions(3)
            .replicas(1)
            .build()

    @Bean
    fun sortingCenterEventsTopic(): NewTopic =
        TopicBuilder
            .name(SORTING_CENTER_EVENTS)
            .partitions(3)
            .replicas(1)
            .build()

    @Bean
    fun parcelDeliveredTopic(): NewTopic =
        TopicBuilder
            .name(PARCEL_DELIVERED)
            .partitions(3)
            .replicas(1)
            .build()

    @Bean
    fun journeyCompletedTopic(): NewTopic =
        TopicBuilder
            .name(JOURNEY_COMPLETED)
            .partitions(3)
            .replicas(1)
            .build()
}
