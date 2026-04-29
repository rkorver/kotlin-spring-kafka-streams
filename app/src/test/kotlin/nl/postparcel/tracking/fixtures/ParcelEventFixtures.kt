package nl.postparcel.tracking.fixtures

import nl.postparcel.tracking.events.v1.Address
import nl.postparcel.tracking.events.v1.DeliveryScan
import nl.postparcel.tracking.events.v1.DeliveryType
import nl.postparcel.tracking.events.v1.ServicePointScan
import nl.postparcel.tracking.events.v1.SortingCenterScan
import nl.postparcel.tracking.events.v1.SortingCenterScanType
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

    fun servicePoint(
        parcelId: String,
        trackingCode: String,
        at: Instant,
        servicePointId: String = "SP-AMS-042",
        senderCity: String = "Amsterdam",
        recipientCity: String = "Utrecht",
        weightGrams: Int = 850,
        employeeId: String = "emp-17",
    ): ServicePointScan =
        ServicePointScan
            .newBuilder()
            .setParcelId(parcelId)
            .setTrackingCode(trackingCode)
            .setServicePointId(servicePointId)
            .setSender(address(senderCity))
            .setRecipient(address(recipientCity))
            .setWeightGrams(weightGrams)
            .setScannedAt(at)
            .setEmployeeId(employeeId)
            .build()

    fun sortingCenterScan(
        parcelId: String,
        trackingCode: String,
        type: SortingCenterScanType,
        at: Instant,
        sortingCenterId: String = "SC-HTN-01",
        belt: String = "B-7",
        destinationHub: String? = if (type == SortingCenterScanType.READY_FOR_DELIVERY) "HUB-UTR" else null,
        remarks: String? = null,
    ): SortingCenterScan =
        SortingCenterScan
            .newBuilder()
            .setParcelId(parcelId)
            .setTrackingCode(trackingCode)
            .setSortingCenterId(sortingCenterId)
            .setScanType(type)
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
        deliveryType: DeliveryType = DeliveryType.TO_DOOR,
        courierId: String = "courier-99",
        signatureBase64: String? = null,
        photoEvidenceUrl: String? = null,
    ): DeliveryScan =
        DeliveryScan
            .newBuilder()
            .setParcelId(parcelId)
            .setTrackingCode(trackingCode)
            .setScannedAt(at)
            .setDeliveryAddress(address(deliveryCity))
            .setDeliveryType(deliveryType)
            .setSignatureBase64(signatureBase64)
            .setCourierId(courierId)
            .setPhotoEvidenceUrl(photoEvidenceUrl)
            .build()
}
