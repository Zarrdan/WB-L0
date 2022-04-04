CREATE TABLE orders
(
    order_uid          varchar PRIMARY KEY NOT NULL,
    track_number       varchar,
    entry              varchar,
    locale             varchar,
    internal_signature varchar,
    customer_id        varchar UNIQUE      NOT NULL,
    delivery_service   varchar,
    shardkey           varchar,
    sm_id              smallint,
    date_created       timestamp,
    oof_shard          varchar
);

CREATE TABLE deliveries
(
    customer_id varchar PRIMARY KEY NOT NULL REFERENCES orders (customer_id) ON DELETE CASCADE,
    name        varchar,
    phone       varchar,
    zip         varchar,
    city        varchar,
    address     varchar,
    region      varchar,
    email       varchar
);

CREATE TABLE payments
(
    order_uid     varchar PRIMARY KEY NOT NULL REFERENCES orders (order_uid) ON DELETE CASCADE,
    transaction   varchar,
    request_id    varchar,
    currency      varchar,
    provider      varchar,
    amount        int,
    payment_dt    int,
    bank          varchar,
    delivery_cost int,
    goods_total   int,
    custom_fee    int
);

CREATE TABLE items
(
    item_id      serial PRIMARY KEY NOT NULL,
    order_uid    varchar            NOT NULL REFERENCES orders (order_uid) ON DELETE CASCADE,
    chrt_id      int,
    track_number varchar,
    price        int,
    rid          varchar,
    name         varchar,
    sale         int,
    size         varchar,
    total_price  int,
    nm_id        int,
    brand        varchar,
    status       int
);


