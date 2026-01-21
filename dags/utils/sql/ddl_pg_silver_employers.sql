CREATE TABLE silver.employers (
    employer_id                 BIGINT PRIMARY KEY,
    name                        TEXT NOT NULL,
    url                         TEXT,
    trusted                     BOOLEAN,
    accredited_it_employer      BOOLEAN,
    country_id                  INT,

    first_seen_dt               DATE NOT NULL,
    last_seen_dt                DATE NOT NULL
);
