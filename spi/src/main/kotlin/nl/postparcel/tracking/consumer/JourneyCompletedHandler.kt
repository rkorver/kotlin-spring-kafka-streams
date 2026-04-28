package nl.postparcel.tracking.consumer

import nl.postparcel.tracking.events.v1.ParcelJourneyCompleted

fun interface JourneyCompletedHandler {
    fun onJourneyCompleted(event: ParcelJourneyCompleted)
}
