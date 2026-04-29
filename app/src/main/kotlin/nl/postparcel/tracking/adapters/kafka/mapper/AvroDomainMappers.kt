package nl.postparcel.tracking.adapters.kafka.mapper

import nl.postparcel.tracking.domain.model.Address
import nl.postparcel.tracking.domain.model.DeliveryEvent
import nl.postparcel.tracking.domain.model.DeliveryType
import nl.postparcel.tracking.domain.model.DeliveryType.MAILBOX
import nl.postparcel.tracking.domain.model.DeliveryType.NEIGHBOUR
import nl.postparcel.tracking.domain.model.DeliveryType.PICKUP_POINT
import nl.postparcel.tracking.domain.model.DeliveryType.TO_DOOR
import nl.postparcel.tracking.domain.model.ReadyForDeliveryEvent
import nl.postparcel.tracking.domain.model.ServicePointEvent
import nl.postparcel.tracking.events.v1.DeliveryScan
import nl.postparcel.tracking.events.v1.ServicePointScan
import nl.postparcel.tracking.events.v1.SortingCenterScan
import nl.postparcel.tracking.events.v1.Address as AvroAddress
import nl.postparcel.tracking.events.v1.DeliveryType as AvroDeliveryType

object AvroDomainMappers {
    fun AvroAddress.toDomain(): Address =
        Address(
            name = name,
            street = street,
            houseNumber = houseNumber,
            postalCode = postalCode,
            city = city,
            country = country,
        )

    fun AvroDeliveryType.toDomain(): DeliveryType =
        when (this) {
            AvroDeliveryType.TO_DOOR -> TO_DOOR
            AvroDeliveryType.NEIGHBOUR -> NEIGHBOUR
            AvroDeliveryType.MAILBOX -> MAILBOX
            AvroDeliveryType.PICKUP_POINT -> PICKUP_POINT
        }

    fun ServicePointScan.toServicePointEvent(): ServicePointEvent =
        ServicePointEvent(
            servicePointId = servicePointId,
            sender = sender.toDomain(),
            recipient = recipient.toDomain(),
            weightGrams = weightGrams,
            scannedAt = scannedAt,
            employeeId = employeeId,
        )

    fun SortingCenterScan.toReadyForDeliveryEvent(): ReadyForDeliveryEvent =
        ReadyForDeliveryEvent(
            sortingCenterId = sortingCenterId,
            destinationHub = destinationHub,
            belt = belt,
            scannedAt = scannedAt,
        )

    fun DeliveryScan.toDeliveryEvent(): DeliveryEvent =
        DeliveryEvent(
            scannedAt = scannedAt,
            deliveryAddress = deliveryAddress.toDomain(),
            deliveryType = deliveryType.toDomain(),
            courierId = courierId,
            signatureBase64 = signatureBase64,
            photoEvidenceUrl = photoEvidenceUrl,
        )
}
