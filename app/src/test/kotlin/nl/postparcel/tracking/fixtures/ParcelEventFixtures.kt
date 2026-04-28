package nl.postparcel.tracking.fixtures

import nl.postparcel.tracking.events.v1.Address
import nl.postparcel.tracking.events.v1.ParcelDeliveredToCustomer
import nl.postparcel.tracking.events.v1.ParcelReceivedAtPostalOffice
import nl.postparcel.tracking.events.v1.ReceiverType
import nl.postparcel.tracking.events.v1.SortingCenterEvent
import nl.postparcel.tracking.events.v1.SortingCenterEventType
import java.time.Instant

object ParcelEventFixtures {
    fun address(city: String = "Amsterdam"): Address =
        Address
            .newBuilder()
            .setName("Jan Jansen")
            .setStreet("Kalverstraat")
            .setHouseNumber("1A")
            .setPostalCode("1012NX")
            .setCity(city)
            .setCountry("NL")
            .build()

    fun postalOffice(
        parcelId: String,
        trackingCode: String,
        at: Instant,
        postalOfficeId: String = "PO-AMS-042",
        postalOfficeCity: String = "Amsterdam",
        senderCity: String = "Amsterdam",
        recipientCity: String = "Utrecht",
        weightGrams: Int = 850,
        employeeId: String = "emp-17",
    ): ParcelReceivedAtPostalOffice =
        ParcelReceivedAtPostalOffice
            .newBuilder()
            .setParcelId(parcelId)
            .setTrackingCode(trackingCode)
            .setPostalOfficeId(postalOfficeId)
            .setPostalOfficeCity(postalOfficeCity)
            .setSender(address(senderCity))
            .setRecipient(address(recipientCity))
            .setWeightGrams(weightGrams)
            .setScannedAt(at)
            .setEmployeeId(employeeId)
            .build()

    fun sortingCenterEvent(
        parcelId: String,
        trackingCode: String,
        type: SortingCenterEventType,
        at: Instant,
        sortingCenterId: String = "SC-HTN-01",
        sortingCenterCity: String = "Houten",
        belt: String = "B-7",
        destinationHub: String? = if (type == SortingCenterEventType.READY_FOR_DELIVERY) "HUB-UTR" else null,
        remarks: String? = null,
    ): SortingCenterEvent =
        SortingCenterEvent
            .newBuilder()
            .setParcelId(parcelId)
            .setTrackingCode(trackingCode)
            .setSortingCenterId(sortingCenterId)
            .setSortingCenterCity(sortingCenterCity)
            .setEventType(type)
            .setDestinationHub(destinationHub)
            .setBelt(belt)
            .setScannedAt(at)
            .setRemarks(remarks)
            .build()

    fun delivered(
        parcelId: String,
        trackingCode: String,
        at: Instant,
        deliveryCity: String = "Utrecht",
        receivedBy: ReceiverType = ReceiverType.CUSTOMER,
        courierId: String = "courier-99",
        signatureBase64: String? = null,
        photoEvidenceUrl: String? = null,
    ): ParcelDeliveredToCustomer =
        ParcelDeliveredToCustomer
            .newBuilder()
            .setParcelId(parcelId)
            .setTrackingCode(trackingCode)
            .setDeliveredAt(at)
            .setDeliveryAddress(address(deliveryCity))
            .setReceivedBy(receivedBy)
            .setSignatureBase64(signatureBase64)
            .setCourierId(courierId)
            .setPhotoEvidenceUrl(photoEvidenceUrl)
            .build()
}
