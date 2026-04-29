package nl.postparcel.tracking.topics

object KafkaTopics {
    const val PARCEL_RECEIVED = "parcel.service-point-scan.v1"
    const val SORTING_CENTER_EVENTS = "parcel.sorting-center-scan.v1"
    const val PARCEL_DELIVERED = "parcel.delivery-scan.v1"
    const val JOURNEY_COMPLETED = "parcel.journey-completed.v1"
}
