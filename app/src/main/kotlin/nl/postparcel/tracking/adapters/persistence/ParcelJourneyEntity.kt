package nl.postparcel.tracking.adapters.persistence

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Id
import jakarta.persistence.Table
import nl.postparcel.tracking.domain.model.ReceiverType
import java.time.Instant

@Entity
@Table(name = "parcel_journey")
class ParcelJourneyEntity(
    @Id
    @Column(name = "parcel_id", nullable = false, updatable = false)
    var parcelId: String = "",
    @Column(name = "tracking_code", nullable = false)
    var trackingCode: String = "",
    @Column(name = "postal_office_id", nullable = false)
    var postalOfficeId: String = "",
    @Column(name = "postal_office_city", nullable = false)
    var postalOfficeCity: String = "",
    @Column(name = "received_at", nullable = false)
    var receivedAt: Instant = Instant.EPOCH,
    @Column(name = "sorting_center_id", nullable = false)
    var sortingCenterId: String = "",
    @Column(name = "sorting_center_city", nullable = false)
    var sortingCenterCity: String = "",
    @Column(name = "ready_for_delivery_at", nullable = false)
    var readyForDeliveryAt: Instant = Instant.EPOCH,
    @Column(name = "delivery_city", nullable = false)
    var deliveryCity: String = "",
    @Column(name = "delivery_postal_code", nullable = false)
    var deliveryPostalCode: String = "",
    @Column(name = "delivered_at", nullable = false)
    var deliveredAt: Instant = Instant.EPOCH,
    @Enumerated(EnumType.STRING)
    @Column(name = "received_by", nullable = false)
    var receivedBy: ReceiverType = ReceiverType.CUSTOMER,
    @Column(name = "courier_id", nullable = false)
    var courierId: String = "",
    @Column(name = "total_duration_ms", nullable = false)
    var totalDurationMillis: Long = 0,
)
