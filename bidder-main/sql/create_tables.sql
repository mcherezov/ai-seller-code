CREATE TABLE wb_bidder_log_v1 (
    dt timestamp NOT NULL,
    calculation_dt timestamp NOT NULL,
    advert_id varchar(64) NOT NULL,
    nm_id varchar(64) NOT NULL,
    duration_sec float NOT NULL,
    cpm_1000 float NOT NULL,
    log text NOT NULL,

    PRIMARY KEY (dt, calculation_dt, nm_id, advert_id)
);

DROP INDEX IF EXISTS advert_id, nm_id, calculation_dt;

CREATE INDEX advert_id ON wb_bidder_log_v1 USING HASH (advert_id);
CREATE INDEX nm_id ON wb_bidder_log_v1 USING HASH (nm_id);
CREATE INDEX calculation_dt ON wb_bidder_log_v1 (calculation_dt);


-- DROP TABLE wb_bidder_log_v1;