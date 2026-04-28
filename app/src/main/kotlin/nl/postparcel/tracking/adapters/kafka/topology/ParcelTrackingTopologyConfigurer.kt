package nl.postparcel.tracking.adapters.kafka.topology

import org.apache.kafka.streams.StreamsBuilder
import org.springframework.stereotype.Component

@Component
class ParcelTrackingTopologyConfigurer(
    topology: ParcelTrackingTopology,
    streamsBuilder: StreamsBuilder,
) {
    init {
        topology.buildTopology(streamsBuilder)
    }
}
