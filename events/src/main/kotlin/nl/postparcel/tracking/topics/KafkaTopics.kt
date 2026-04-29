package nl.postparcel.tracking.topics

object KafkaTopics {
    const val PARCEL_RECEIVED = "parcel.received-at-postal-office.v1"
    const val SORTING_CENTER_EVENTS = "parcel.sorting-center-events.v1"
    const val PARCEL_DELIVERED = "parcel.delivered-to-customer.v1"
    const val JOURNEY_COMPLETED = "parcel.journey-completed.v1"
}
