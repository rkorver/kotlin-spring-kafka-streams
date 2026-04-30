package nl.postparcel.tracking.adapters.web

import io.mockk.every
import io.mockk.mockk
import nl.postparcel.tracking.domain.model.Address
import nl.postparcel.tracking.domain.model.CompletedParcelJourney
import nl.postparcel.tracking.domain.model.DeliveryEvent
import nl.postparcel.tracking.domain.model.DeliveryType
import nl.postparcel.tracking.domain.model.ParcelId
import nl.postparcel.tracking.domain.model.ReadyForDeliveryEvent
import nl.postparcel.tracking.domain.model.ServicePointEvent
import nl.postparcel.tracking.domain.model.TrackingCode
import nl.postparcel.tracking.domain.port.inbound.ParcelJourneyPort
import org.junit.jupiter.api.Test
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import java.time.Duration
import java.time.Instant

class ParcelJourneyControllerTest {
    private val parcelJourneyPort: ParcelJourneyPort = mockk()
    private val mockMvc: MockMvc =
        MockMvcBuilders
            .standaloneSetup(ParcelJourneyController(parcelJourneyPort))
            .build()

    @Test
    fun `returns 200 with journey response when found`() {
        val journey = aCompletedJourney()
        every { parcelJourneyPort.getParcelJourneyById(ParcelId("parcel-1")) } returns journey

        mockMvc.get("/parcels/parcel-1").andExpect {
            status { isOk() }
            jsonPath("$.parcelId") { value("parcel-1") }
            jsonPath("$.trackingCode") { value("3SKABA999") }
            jsonPath("$.servicePointId") { value("SP-01") }
            jsonPath("$.sortingCenterId") { value("SC-01") }
            jsonPath("$.deliveryCity") { value("Rotterdam") }
            jsonPath("$.deliveryType") { value("TO_DOOR") }
            jsonPath("$.totalDurationMillis") { value(18000000) }
        }
    }

    @Test
    fun `returns 404 when journey not found`() {
        every { parcelJourneyPort.getParcelJourneyById(ParcelId("nonexistent")) } returns null

        mockMvc.get("/parcels/nonexistent").andExpect {
            status { isNotFound() }
        }
    }

    private fun aCompletedJourney() =
        CompletedParcelJourney(
            parcelId = ParcelId("parcel-1"),
            trackingCode = TrackingCode("3SKABA999"),
            servicePoint =
                ServicePointEvent(
                    servicePointId = "SP-01",
                    sender = Address("A", "Street", "1", "1000AA", "Amsterdam", "NL"),
                    recipient = Address("B", "Lane", "2", "3011AB", "Rotterdam", "NL"),
                    weightGrams = 500,
                    scannedAt = Instant.parse("2026-04-20T09:00:00Z"),
                    employeeId = null,
                ),
            readyForDelivery =
                ReadyForDeliveryEvent(
                    sortingCenterId = "SC-01",
                    destinationHub = null,
                    belt = null,
                    scannedAt = Instant.parse("2026-04-20T12:00:00Z"),
                ),
            delivered =
                DeliveryEvent(
                    scannedAt = Instant.parse("2026-04-20T14:00:00Z"),
                    deliveryAddress = Address("B", "Lane", "2", "3011AB", "Rotterdam", "NL"),
                    deliveryType = DeliveryType.TO_DOOR,
                    courierId = "courier-1",
                    signatureBase64 = null,
                    photoEvidenceUrl = null,
                ),
            totalDuration = Duration.ofHours(5),
        )
}
