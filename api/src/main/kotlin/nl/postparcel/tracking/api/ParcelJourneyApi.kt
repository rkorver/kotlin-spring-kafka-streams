package nl.postparcel.tracking.api

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping

@RequestMapping("/parcels")
interface ParcelJourneyApi {
    @GetMapping("/{parcelId}")
    fun getJourney(
        @PathVariable("parcelId") parcelId: String,
    ): ResponseEntity<ParcelJourneyResponse>
}
