package nl.postparcel.tracking.domain.model

@JvmInline
value class ParcelId(
    val value: String,
) {
    init {
        require(value.isNotBlank()) { "ParcelId must not be blank" }
    }

    override fun toString(): String = value
}
