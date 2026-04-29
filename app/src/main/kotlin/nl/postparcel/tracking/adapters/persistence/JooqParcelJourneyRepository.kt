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
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL.field
import org.jooq.impl.DSL.table
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset

@Component
class JooqParcelJourneyRepository(
    private val dsl: DSLContext,
) : ParcelJourneyRepository {
    override fun save(journey: CompletedParcelJourney): CompletedParcelJourney {
        dsl
            .insertInto(PARCEL_JOURNEY)
            .set(PARCEL_ID, journey.parcelId.value)
            .set(TRACKING_CODE, journey.trackingCode.value)
            .set(SERVICE_POINT_ID, journey.servicePoint.servicePointId)
            .set(RECEIVED_AT, journey.servicePoint.scannedAt.toUtcLocalDateTime())
            .set(SORTING_CENTER_ID, journey.readyForDelivery.sortingCenterId)
            .set(READY_FOR_DELIVERY_AT, journey.readyForDelivery.scannedAt.toUtcLocalDateTime())
            .set(DELIVERY_CITY, journey.delivered.deliveryAddress.city)
            .set(DELIVERY_POSTAL_CODE, journey.delivered.deliveryAddress.postalCode)
            .set(DELIVERED_AT, journey.delivered.scannedAt.toUtcLocalDateTime())
            .set(DELIVERY_TYPE, journey.delivered.deliveryType.name)
            .set(COURIER_ID, journey.delivered.courierId)
            .set(TOTAL_DURATION_MS, journey.totalDuration.toMillis())
            .onConflict(PARCEL_ID)
            .doUpdate()
            .set(TRACKING_CODE, journey.trackingCode.value)
            .set(SERVICE_POINT_ID, journey.servicePoint.servicePointId)
            .set(RECEIVED_AT, journey.servicePoint.scannedAt.toUtcLocalDateTime())
            .set(SORTING_CENTER_ID, journey.readyForDelivery.sortingCenterId)
            .set(READY_FOR_DELIVERY_AT, journey.readyForDelivery.scannedAt.toUtcLocalDateTime())
            .set(DELIVERY_CITY, journey.delivered.deliveryAddress.city)
            .set(DELIVERY_POSTAL_CODE, journey.delivered.deliveryAddress.postalCode)
            .set(DELIVERED_AT, journey.delivered.scannedAt.toUtcLocalDateTime())
            .set(DELIVERY_TYPE, journey.delivered.deliveryType.name)
            .set(COURIER_ID, journey.delivered.courierId)
            .set(TOTAL_DURATION_MS, journey.totalDuration.toMillis())
            .execute()
        return journey
    }

    override fun findByParcelId(parcelId: ParcelId): CompletedParcelJourney? =
        dsl
            .selectFrom(PARCEL_JOURNEY)
            .where(PARCEL_ID.eq(parcelId.value))
            .fetchOne()
            ?.toDomain()

    private fun Record.toDomain(): CompletedParcelJourney {
        val deliveryCity = get(DELIVERY_CITY, String::class.java)
        return CompletedParcelJourney(
            parcelId = ParcelId(get(PARCEL_ID, String::class.java)),
            trackingCode = TrackingCode(get(TRACKING_CODE, String::class.java)),
            servicePoint =
                ServicePointEvent(
                    servicePointId = get(SERVICE_POINT_ID, String::class.java),
                    sender = Address("", "", "", "", "", "NL"),
                    recipient = Address("", "", "", "", deliveryCity, "NL"),
                    weightGrams = 0,
                    scannedAt =
                        get(RECEIVED_AT, LocalDateTime::class.java)
                            .toInstant(ZoneOffset.UTC),
                    employeeId = null,
                ),
            readyForDelivery =
                ReadyForDeliveryEvent(
                    sortingCenterId = get(SORTING_CENTER_ID, String::class.java),
                    destinationHub = null,
                    belt = null,
                    scannedAt =
                        get(READY_FOR_DELIVERY_AT, LocalDateTime::class.java)
                            .toInstant(ZoneOffset.UTC),
                ),
            delivered =
                DeliveryEvent(
                    scannedAt =
                        get(DELIVERED_AT, LocalDateTime::class.java)
                            .toInstant(ZoneOffset.UTC),
                    deliveryAddress =
                        Address(
                            "",
                            "",
                            "",
                            get(DELIVERY_POSTAL_CODE, String::class.java),
                            deliveryCity,
                            "NL",
                        ),
                    deliveryType = DeliveryType.valueOf(get(DELIVERY_TYPE, String::class.java)),
                    courierId = get(COURIER_ID, String::class.java),
                    signatureBase64 = null,
                    photoEvidenceUrl = null,
                ),
            totalDuration = Duration.ofMillis(get(TOTAL_DURATION_MS, Long::class.java)),
        )
    }

    companion object {
        private val PARCEL_JOURNEY = table("parcel_journey")
        private val PARCEL_ID = field("parcel_id", String::class.java)
        private val TRACKING_CODE = field("tracking_code", String::class.java)
        private val SERVICE_POINT_ID = field("service_point_id", String::class.java)
        private val RECEIVED_AT = field("received_at", LocalDateTime::class.java)
        private val SORTING_CENTER_ID = field("sorting_center_id", String::class.java)
        private val READY_FOR_DELIVERY_AT = field("ready_for_delivery_at", LocalDateTime::class.java)
        private val DELIVERY_CITY = field("delivery_city", String::class.java)
        private val DELIVERY_POSTAL_CODE = field("delivery_postal_code", String::class.java)
        private val DELIVERED_AT = field("delivered_at", LocalDateTime::class.java)
        private val DELIVERY_TYPE = field("delivery_type", String::class.java)
        private val COURIER_ID = field("courier_id", String::class.java)
        private val TOTAL_DURATION_MS = field("total_duration_ms", Long::class.java)
    }
}

private fun java.time.Instant.toUtcLocalDateTime(): LocalDateTime = atZone(ZoneOffset.UTC).toLocalDateTime()
