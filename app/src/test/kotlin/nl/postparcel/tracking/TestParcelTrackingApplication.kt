package nl.postparcel.tracking

import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.core.env.MapPropertySource
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.redpanda.RedpandaContainer
import nl.postparcel.tracking.topics.KafkaTopics
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.testcontainers.utility.DockerImageName

class TestcontainersInitializer : ApplicationContextInitializer<ConfigurableApplicationContext> {

    companion object {
        val postgres = PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine"))
            .withDatabaseName("parcel_tracking")
            .withUsername("parcel")
            .withPassword("parcel")

        val redpanda = RedpandaContainer(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v24.3.1"))

        init {
            postgres.start()
            redpanda.start()
            createTopics()
        }

        private fun createTopics() {
            AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to redpanda.bootstrapServers)).use { admin ->
                val topics = listOf(
                    KafkaTopics.PARCEL_RECEIVED,
                    KafkaTopics.SORTING_CENTER_EVENTS,
                    KafkaTopics.PARCEL_DELIVERED,
                    KafkaTopics.JOURNEY_COMPLETED,
                ).map { NewTopic(it, 3, 1.toShort()) }
                admin.createTopics(topics).all().get()
            }
        }
    }

    override fun initialize(ctx: ConfigurableApplicationContext) {
        ctx.environment.propertySources.addFirst(
            MapPropertySource(
                "testcontainers",
                mapOf(
                    "spring.datasource.url" to postgres.jdbcUrl,
                    "spring.datasource.username" to postgres.username,
                    "spring.datasource.password" to postgres.password,
                    "spring.kafka.bootstrap-servers" to redpanda.bootstrapServers,
                    "spring.kafka.properties.schema.registry.url" to redpanda.schemaRegistryAddress,
                ),
            ),
        )
    }
}

fun main(args: Array<String>) {
    SpringApplicationBuilder(PostParcelTrackingApplication::class.java)
        .initializers(TestcontainersInitializer())
        .run(*args)
}
