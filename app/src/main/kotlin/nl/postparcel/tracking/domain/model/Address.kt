package nl.postparcel.tracking.domain.model

data class Address(
    val name: String,
    val street: String,
    val houseNumber: String,
    val postalCode: String,
    val city: String,
    val country: String,
)
