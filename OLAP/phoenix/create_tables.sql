CREATE TABLE IF NOT EXISTS BICING_DIM_STATION (
    ID UNSIGNED_LONG NOT NULL,
    -- geo info
    GEO.LONGITUDE UNSIGNED_DOUBLE,
    GEO.LATITUDE UNSIGNED_DOUBLE,
    GEO.HEIGH UNSIGNED_LONG,
    -- human readable location 
    LOC.DISTRICT VARCHAR,
    LOC.NEIGHBORHOOD VARCHAR,
    LOC.POSTAL_CODE VARCHAR,
    LOC.ADDRESS VARCHAR,
    -- district info
    DISTRICT.POP_DENSITY UNSIGNED_DOUBLE,
    DISTRICT.POPULATION UNSIGNED_LONG,
    DISTRICT.SIZE UNSIGNED_DOUBLE
    -- 
    CONSTRAINT PK PRIMARY KEY (ID)
);

CREATE TABLE IF NOT EXISTS BICING_DIM_TIME (  
    -- native apache phoenix meaning: The format is yyyy-MM-dd hh:mm:ss[.nnnnnnnnn]
    -- Mapped to java.sql.Timestamp with an internal representation 
    -- of the number of nanos from the epoch
    TIMETAG TIMESTAMP NOT NULL,
    -- small values, all in the same column
    -- 0 to 59
    D.MINUTE UNSIGNED_TINYINT, 
    -- 0 to 23
    D.HOUR UNSIGNED_TINYINT, 
    -- VALUES: '[04:00 - 08:00)', '[08:00 - 12:00)', 
    --         '[12:00 - 16:00)', '[16:00 - 20:00)', 
    --         '[20:00 - 00:00)', '[00:00 - 04:00)'
    D.DAYSIXTH VARCHAR(15),
    -- Day parts: http://en.wikipedia.org/wiki/Rush_hour
    -- VALUE: 'GO-WORK' (rush hour going to work: [06:00 - 10:00)), 
    -- 'MORNING' ([10:00 - 13:00)), 'LUNCH' (spanish lunch [13:00, 15:00)),
    -- 'AFTERNOON' (afternoon, [15:00 - 17:00), 'BACK-HOME' (spanish rush hour for 
    -- going back home: [17:00 - 19:00), 'NIGHT' [19:00, 06:00)
    D.PART VARCHAR(9),
    -- 1 to 30
    D.MONTH_DAY UNSIGNED_TINYINT, 
    -- 1 to 365
    D.YEAR_DAY UNSIGNED_SMALLINT, 
    -- 1 to 5
    D.MONTH_WEEK UNSIGNED_TINYINT, 
    -- 1 to 53
    D.YEAR_WEEK UNSIGNED_TINYINT, 
    -- 1 to 12
    D.MONTH UNSIGNED_TINYINT, 
    -- 1 to 4
    D.TRIMESTER UNSIGNED_TINYINT, 
    -- e.g. 2014
    D.YEAR UNSIGNED_SMALLINT
    -- This table should be salted to prevent 
    -- region server hot spotting on inserts,
    -- as the row key is monotonically increasing
    CONSTRAINT PK PRIMARY KEY (TIMETAG)
) SALT_BUCKETS = 20;

-- The total number of stations cannot be computed in this 
-- table
CREATE TABLE IF NOT EXISTS BICING_FACT (
    -- station to which the data for this row applies
    -- FK for BICING_DIM_STATION
    STATION UNSIGNED_LONG NOT NULL,
    -- NOTE: remember bicing ingests UNIX time in seconds
    -- FK for BICING_DIM_TIME
    TIMETAG TIMESTAMP NOT NULL,
    -- Fact fields
    -- As there are just a few fact we put all in the same HBase column
    -- Degenerate dimension: 'OPN' (open) or 'CLS' (closed)
    F.STATUS VARCHAR(3),
    -- Number of parking slots available: should be 0 
    -- if F.STATUS is "CLS"
    F.SLOTS UNSIGNED_LONG,
    -- Number of bikes available: should be 0 
    -- if F.STATUS is "CLS"
    F.AVAILABLE UNSIGNED_LONG,
    -- Total capacity of the station as (parking slots
    -- + bikes) * (status == OPN)
    F.CAPACITY UNSIGNED_LONG,
    -- Number of bikes lent for this station since 
    -- the previous update
    F.LENT UNSIGNED_LONG,
    -- Number of bikes returned to this station since 
    -- the previous update
    F.RETURNED UNSIGNED_LONG
    -- This gives a non monotonically increasing row key
    CONSTRAINT PK PRIMARY KEY (STATION, TIMETAG)
);

-- Clear tables with

-- delete from BICING_FACT ;
-- delete from BICING_DIM_TIME;