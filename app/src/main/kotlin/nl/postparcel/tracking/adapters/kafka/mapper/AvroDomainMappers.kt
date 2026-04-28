package nl.postparcel.tracking.adapters.kafka.mapper

import nl.postparcel.tracking.domain.model.Address
import nl.postparcel.tracking.domain.model.DeliveryScan
import nl.postparcel.tracking.domain.model.PostalOfficeScan
import nl.postparcel.tracking.domain.model.ReadyForDeliveryScan
import nl.postparcel.tracking.domain.model.ReceiverType
import nl.postparcel.tracking.domain.model.ReceiverType.CUSTOMER
import nl.postparcel.tracking.domain.model.ReceiverType.MAILBOX
import nl.postparcel.tracking.domain.model.ReceiverType.NEIGHBOUR
import nl.postparcel.tracking.domain.model.ReceiverType.PICKUP_POINT
import nl.postparcel.tracking.events.v1.ParcelDeliveredToCustomer
import nl.postparcel.tracking.events.v1.ParcelReceivedAtPostalOffice
import nl.postparcel.tracking.events.v1.SortingCenterEvent
import nl.postparcel.tracking.events.v1.Address as AvroAddress
import nl.postparcel.tracking.events.v1.ReceiverType as AvroReceiverType

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

    fun AvroReceiverType.toDomain(): ReceiverType =
        when (this) {
            AvroReceiverType.CUSTOMER -> CUSTOMER
            AvroReceiverType.NEIGHBOUR -> NEIGHBOUR
            AvroReceiverType.MAILBOX -> MAILBOX
            AvroReceiverType.PICKUP_POINT -> PICKUP_POINT
        }

    fun ParcelReceivedAtPostalOffice.toPostalOfficeScan(): PostalOfficeScan =
        PostalOfficeScan(
            postalOfficeId = postalOfficeId,
            postalOfficeCity = postalOfficeCity,
            sender = sender.toDomain(),
            recipient = recipient.toDomain(),
            weightGrams = weightGrams,
            scannedAt = scannedAt,
            employeeId = employeeId,
        )

    fun SortingCenterEvent.toReadyForDeliveryScan(): ReadyForDeliveryScan =
        ReadyForDeliveryScan(
            sortingCenterId = sortingCenterId,
            sortingCenterCity = sortingCenterCity,
            destinationHub = destinationHub,
            belt = belt,
            scannedAt = scannedAt,
        )

    fun ParcelDeliveredToCustomer.toDeliveryScan(): DeliveryScan =
        DeliveryScan(
            deliveredAt = deliveredAt,
            deliveryAddress = deliveryAddress.toDomain(),
            receivedBy = receivedBy.toDomain(),
            courierId = courierId,
            signatureBase64 = signatureBase64,
            photoEvidenceUrl = photoEvidenceUrl,
        )
}
