package nl.postparcel.tracking.adapters.kafka.config

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import nl.postparcel.tracking.adapters.kafka.serde.AvroSerdes
import nl.postparcel.tracking.events.v1.DeliveryScan
import nl.postparcel.tracking.events.v1.ParcelJourneyCompleted
import nl.postparcel.tracking.events.v1.ParcelJourneyState
import nl.postparcel.tracking.events.v1.ServicePointScan
import nl.postparcel.tracking.events.v1.SortingCenterScan
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG
import org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE
import org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG
import org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration

@Configuration
class KafkaStreamsConfig(
    @param:Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @param:Value("\${spring.kafka.properties.schema.registry.url}") private val schemaRegistryUrl: String,
    @param:Value("\${spring.application.name:parcel-tracking}") private val applicationName: String,
    @param:Value(
        "\${spring.kafka.streams.state-dir:#{T(System).getProperty('java.io.tmpdir') + '/kafka-streams'}}",
    )
    private val stateDir: String,
) {
    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kafkaStreamsConfiguration() =
        KafkaStreamsConfiguration(
            mapOf(
                BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
                APPLICATION_ID_CONFIG to applicationName,
                DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                DEFAULT_VALUE_SERDE_CLASS_CONFIG to SpecificAvroSerde::class.java.name,
                PROCESSING_GUARANTEE_CONFIG to AT_LEAST_ONCE,
                SPECIFIC_AVRO_READER_CONFIG to true,
                AUTO_OFFSET_RESET_CONFIG to "earliest",
                ACKS_CONFIG to "all",
                STATE_DIR_CONFIG to stateDir,
            ),
        )

    @Bean
    fun avroSerdes(): AvroSerdes = AvroSerdes(schemaRegistryUrl)

    @Bean
    fun stringSerdes(): Serde<String> = Serdes.String()

    @Bean
    fun servicePointScanSerde(serdes: AvroSerdes): SpecificAvroSerde<ServicePointScan> = serdes.create()

    @Bean
    fun sortingCenterScanSerde(serdes: AvroSerdes): SpecificAvroSerde<SortingCenterScan> = serdes.create()

    @Bean
    fun deliveryScanSerde(serdes: AvroSerdes): SpecificAvroSerde<DeliveryScan> = serdes.create()

    @Bean
    fun parcelJourneyStateSerde(serdes: AvroSerdes): SpecificAvroSerde<ParcelJourneyState> = serdes.create()

    @Bean
    fun parcelJourneyCompletedSerde(serdes: AvroSerdes): SpecificAvroSerde<ParcelJourneyCompleted> = serdes.create()
}
