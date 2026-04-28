package nl.postparcel.tracking.adapters.kafka.serde

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord

class AvroSerdes(
    schemaRegistryUrl: String,
) {
    private val config =
        mapOf(
            SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
            SPECIFIC_AVRO_READER_CONFIG to "true",
        )

    fun <T : SpecificRecord> create() =
        SpecificAvroSerde<T>()
            .apply { configure(config, false) }
}
