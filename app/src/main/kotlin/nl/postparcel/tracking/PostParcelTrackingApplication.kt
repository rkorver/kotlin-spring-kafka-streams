package nl.postparcel.tracking

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams

@EnableKafka
@EnableKafkaStreams
@SpringBootApplication
class PostParcelTrackingApplication

fun main(args: Array<String>) {
    runApplication<PostParcelTrackingApplication>(*args)
}
