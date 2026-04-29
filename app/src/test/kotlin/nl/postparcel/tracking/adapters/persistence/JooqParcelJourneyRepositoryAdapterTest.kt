package nl.postparcel.tracking.adapters.persistence

import nl.postparcel.tracking.domain.model.Address
import nl.postparcel.tracking.domain.model.CompletedParcelJourney
import nl.postparcel.tracking.domain.model.DeliveryEvent
import nl.postparcel.tracking.domain.model.DeliveryType
import nl.postparcel.tracking.domain.model.ParcelId
import nl.postparcel.tracking.domain.model.ReadyForDeliveryEvent
import nl.postparcel.tracking.domain.model.ServicePointEvent
import nl.postparcel.tracking.domain.model.TrackingCode
import nl.postparcel.tracking.domain.port.ParcelJourneyRepository
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration
import org.springframework.boot.jooq.autoconfigure.JooqAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.Duration
import java.time.Instant

@SpringBootTest(
    classes = [JooqParcelJourneyRepository::class],
)
@ImportAutoConfiguration(
    DataSourceAutoConfiguration::class,
    JooqAutoConfiguration::class,
    FlywayAutoConfiguration::class,
)
@Testcontainers
class JooqParcelJourneyRepositoryAdapterTest {
    @Autowired
    lateinit var repository: ParcelJourneyRepository

    @Test
    fun `save and retrieve a completed journey`() {
        val journey = aCompletedJourney()

        repository.save(journey)

        val found = repository.findByParcelId(journey.parcelId)
        assertThat(found).isNotNull
        assertThat(found!!.parcelId).isEqualTo(journey.parcelId)
        assertThat(found.trackingCode).isEqualTo(journey.trackingCode)
        assertThat(found.servicePoint.servicePointId).isEqualTo("office-1")
        assertThat(found.servicePoint.scannedAt).isEqualTo(journey.servicePoint.scannedAt)
        assertThat(found.readyForDelivery.sortingCenterId).isEqualTo("sc-1")
        assertThat(found.readyForDelivery.scannedAt).isEqualTo(journey.readyForDelivery.scannedAt)
        assertThat(found.delivered.deliveryAddress.city).isEqualTo("Rotterdam")
        assertThat(found.delivered.deliveryAddress.postalCode).isEqualTo("3011AB")
        assertThat(found.delivered.scannedAt).isEqualTo(journey.delivered.scannedAt)
        assertThat(found.delivered.deliveryType).isEqualTo(DeliveryType.NEIGHBOUR)
        assertThat(found.delivered.courierId).isEqualTo("courier-42")
        assertThat(found.totalDuration).isEqualTo(journey.totalDuration)
    }

    @Test
    fun `findByParcelId returns null for unknown id`() {
        assertThat(repository.findByParcelId(ParcelId("nonexistent"))).isNull()
    }

    @Test
    fun `save overwrites existing journey`() {
        val journey = aCompletedJourney()
        repository.save(journey)

        val updated = journey.copy(trackingCode = TrackingCode("UPDATED123"))
        repository.save(updated)

        val found = repository.findByParcelId(journey.parcelId)
        assertThat(found!!.trackingCode).isEqualTo(TrackingCode("UPDATED123"))
    }

    companion object {
        @Container
        @JvmStatic
        val postgres: PostgreSQLContainer =
            PostgreSQLContainer("postgres:17-alpine")
                .withDatabaseName("parcel_tracking")
                .withUsername("parcel")
                .withPassword("parcel")

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") { postgres.jdbcUrl }
            registry.add("spring.datasource.username") { postgres.username }
            registry.add("spring.datasource.password") { postgres.password }
            registry.add("spring.flyway.enabled") { "true" }
        }
    }
}

private fun aCompletedJourney() =
    CompletedParcelJourney(
        parcelId = ParcelId("parcel-test-123"),
        trackingCode = TrackingCode("3SKABA999"),
        servicePoint =
            ServicePointEvent(
                servicePointId = "office-1",
                sender = Address("", "", "", "", "", "NL"),
                recipient = Address("", "", "", "", "Rotterdam", "NL"),
                weightGrams = 500,
                scannedAt = Instant.parse("2026-04-20T09:00:00Z"),
                employeeId = null,
            ),
        readyForDelivery =
            ReadyForDeliveryEvent(
                sortingCenterId = "sc-1",
                destinationHub = null,
                belt = null,
                scannedAt = Instant.parse("2026-04-21T03:00:00Z"),
            ),
        delivered =
            DeliveryEvent(
                scannedAt = Instant.parse("2026-04-21T14:00:00Z"),
                deliveryAddress = Address("", "", "", "3011AB", "Rotterdam", "NL"),
                deliveryType = DeliveryType.NEIGHBOUR,
                courierId = "courier-42",
                signatureBase64 = null,
                photoEvidenceUrl = null,
            ),
        totalDuration =
            Duration.between(
                Instant.parse("2026-04-20T09:00:00Z"),
                Instant.parse("2026-04-21T14:00:00Z"),
            ),
    )
