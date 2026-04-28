package nl.postparcel.tracking.domain.model

@JvmInline
value class TrackingCode(
    val value: String,
) {
    init {
        require(value.isNotBlank()) { "TrackingCode must not be blank" }
    }

    override fun toString(): String = value
}
