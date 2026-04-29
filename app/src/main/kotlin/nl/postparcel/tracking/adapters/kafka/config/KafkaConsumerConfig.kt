package nl.postparcel.tracking.adapters.kafka.config

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory

@Configuration
class KafkaConsumerConfig(
    @param:Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @param:Value("\${spring.kafka.properties.schema.registry.url}") private val schemaRegistryUrl: String,
) {
    @Bean
    fun avroConsumerFactory(): ConsumerFactory<String, Any> =
        DefaultKafkaConsumerFactory(
            mapOf(
                BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
                KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
                SPECIFIC_AVRO_READER_CONFIG to true,
                AUTO_OFFSET_RESET_CONFIG to "earliest",
            ),
        )

    @Bean
    fun avroKafkaListenerContainerFactory(avroConsumerFactory: ConsumerFactory<String, Any>) =
        ConcurrentKafkaListenerContainerFactory<String, Any>()
            .apply { setConsumerFactory(avroConsumerFactory) }
}
