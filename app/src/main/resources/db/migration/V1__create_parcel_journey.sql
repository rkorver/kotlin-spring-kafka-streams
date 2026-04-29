CREATE TABLE parcel_journey (
    parcel_id              VARCHAR(64)  PRIMARY KEY,
    tracking_code          VARCHAR(32)  NOT NULL,
    service_point_id       VARCHAR(64)  NOT NULL,
    received_at            TIMESTAMP    NOT NULL,
    sorting_center_id      VARCHAR(64)  NOT NULL,
    ready_for_delivery_at  TIMESTAMP    NOT NULL,
    delivery_city          VARCHAR(128) NOT NULL,
    delivery_postal_code   VARCHAR(16)  NOT NULL,
    delivered_at           TIMESTAMP    NOT NULL,
    delivery_type          VARCHAR(32)  NOT NULL,
    courier_id             VARCHAR(64)  NOT NULL,
    total_duration_ms      BIGINT       NOT NULL
);

CREATE INDEX idx_parcel_journey_tracking_code ON parcel_journey (tracking_code);
CREATE INDEX idx_parcel_journey_delivered_at ON parcel_journey (delivered_at);
