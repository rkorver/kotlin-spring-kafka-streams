package nl.postparcel.tracking.adapters.kafka.mapper

import nl.postparcel.tracking.adapters.kafka.mapper.AvroDomainMappers.toDeliveryEvent
import nl.postparcel.tracking.adapters.kafka.mapper.AvroDomainMappers.toDomain
import nl.postparcel.tracking.adapters.kafka.mapper.AvroDomainMappers.toReadyForDeliveryEvent
import nl.postparcel.tracking.adapters.kafka.mapper.AvroDomainMappers.toServicePointEvent
import nl.postparcel.tracking.adapters.kafka.mapper.CompletedJourneyMapper.toDomain
import nl.postparcel.tracking.domain.model.DeliveryType
import nl.postparcel.tracking.events.v1.Address
import nl.postparcel.tracking.events.v1.DeliveryScan
import nl.postparcel.tracking.events.v1.ParcelJourneyCompleted
import nl.postparcel.tracking.events.v1.ServicePointScan
import nl.postparcel.tracking.events.v1.SortingCenterScan
import nl.postparcel.tracking.events.v1.SortingCenterScanType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.time.Duration
import java.time.Instant
import nl.postparcel.tracking.events.v1.DeliveryType as AvroDeliveryType

class AvroDomainMappersTest {
    private val now = Instant.parse("2026-04-20T10:00:00Z")

    private fun avroAddress(city: String = "Amsterdam") =
        Address
            .newBuilder()
            .setName("Test")
            .setStreet("Straat")
            .setHouseNumber("42B")
            .setPostalCode("1234AB")
            .setCity(city)
            .setCountry("NL")
            .build()

    @Test
    fun `Address maps all fields`() {
        val result = avroAddress("Rotterdam").toDomain()
        assertThat(result.name).isEqualTo("Test")
        assertThat(result.street).isEqualTo("Straat")
        assertThat(result.houseNumber).isEqualTo("42B")
        assertThat(result.postalCode).isEqualTo("1234AB")
        assertThat(result.city).isEqualTo("Rotterdam")
        assertThat(result.country).isEqualTo("NL")
    }

    @ParameterizedTest
    @EnumSource(AvroDeliveryType::class)
    fun `all DeliveryType variants map correctly`(avroType: AvroDeliveryType) {
        val result = avroType.toDomain()
        assertThat(result.name).isEqualTo(avroType.name)
    }

    @Test
    fun `ServicePointScan maps including nullable employeeId`() {
        val scan =
            ServicePointScan
                .newBuilder()
                .setParcelId("p1")
                .setTrackingCode("TC1")
                .setServicePointId("SP-1")
                .setSender(avroAddress())
                .setRecipient(avroAddress("Utrecht"))
                .setWeightGrams(1200)
                .setScannedAt(now)
                .setEmployeeId(null)
                .build()

        val result = scan.toServicePointEvent()
        assertThat(result.servicePointId).isEqualTo("SP-1")
        assertThat(result.weightGrams).isEqualTo(1200)
        assertThat(result.scannedAt).isEqualTo(now)
        assertThat(result.employeeId).isNull()
        assertThat(result.recipient.city).isEqualTo("Utrecht")
    }

    @Test
    fun `SortingCenterScan maps nullable fields`() {
        val scan =
            SortingCenterScan
                .newBuilder()
                .setParcelId("p1")
                .setTrackingCode("TC1")
                .setSortingCenterId("SC-1")
                .setScanType(SortingCenterScanType.READY_FOR_DELIVERY)
                .setDestinationHub(null)
                .setBelt(null)
                .setScannedAt(now)
                .setRemarks(null)
                .build()

        val result = scan.toReadyForDeliveryEvent()
        assertThat(result.sortingCenterId).isEqualTo("SC-1")
        assertThat(result.destinationHub).isNull()
        assertThat(result.belt).isNull()
        assertThat(result.scannedAt).isEqualTo(now)
    }

    @Test
    fun `DeliveryScan maps nullable signature and photo`() {
        val scan =
            DeliveryScan
                .newBuilder()
                .setParcelId("p1")
                .setTrackingCode("TC1")
                .setScannedAt(now)
                .setDeliveryAddress(avroAddress())
                .setDeliveryType(AvroDeliveryType.NEIGHBOUR)
                .setCourierId("c-1")
                .setSignatureBase64("abc123")
                .setPhotoEvidenceUrl("https://example.com/photo.jpg")
                .build()

        val result = scan.toDeliveryEvent()
        assertThat(result.deliveryType).isEqualTo(DeliveryType.NEIGHBOUR)
        assertThat(result.courierId).isEqualTo("c-1")
        assertThat(result.signatureBase64).isEqualTo("abc123")
        assertThat(result.photoEvidenceUrl).isEqualTo("https://example.com/photo.jpg")
    }

    @Test
    fun `CompletedJourneyMapper maps full event to domain`() {
        val event =
            ParcelJourneyCompleted
                .newBuilder()
                .setParcelId("parcel-99")
                .setTrackingCode("3SKABA999")
                .setServicePoint(
                    ServicePointScan
                        .newBuilder()
                        .setParcelId("parcel-99")
                        .setTrackingCode("3SKABA999")
                        .setServicePointId("SP-1")
                        .setSender(avroAddress())
                        .setRecipient(avroAddress())
                        .setWeightGrams(500)
                        .setScannedAt(now)
                        .setEmployeeId("emp-1")
                        .build(),
                ).setReadyForDelivery(
                    SortingCenterScan
                        .newBuilder()
                        .setParcelId("parcel-99")
                        .setTrackingCode("3SKABA999")
                        .setSortingCenterId("SC-1")
                        .setScanType(SortingCenterScanType.READY_FOR_DELIVERY)
                        .setDestinationHub("HUB-1")
                        .setBelt("B-3")
                        .setScannedAt(now.plusSeconds(3600))
                        .setRemarks(null)
                        .build(),
                ).setDelivered(
                    DeliveryScan
                        .newBuilder()
                        .setParcelId("parcel-99")
                        .setTrackingCode("3SKABA999")
                        .setScannedAt(now.plusSeconds(7200))
                        .setDeliveryAddress(avroAddress())
                        .setDeliveryType(AvroDeliveryType.MAILBOX)
                        .setCourierId("c-5")
                        .setSignatureBase64(null)
                        .setPhotoEvidenceUrl(null)
                        .build(),
                ).setTotalDurationMillis(7200_000L)
                .build()

        val result = event.toDomain()
        assertThat(result.parcelId.value).isEqualTo("parcel-99")
        assertThat(result.trackingCode.value).isEqualTo("3SKABA999")
        assertThat(result.totalDuration).isEqualTo(Duration.ofHours(2))
        assertThat(result.readyForDelivery.destinationHub).isEqualTo("HUB-1")
        assertThat(result.readyForDelivery.belt).isEqualTo("B-3")
        assertThat(result.delivered.deliveryType).isEqualTo(DeliveryType.MAILBOX)
    }
}
