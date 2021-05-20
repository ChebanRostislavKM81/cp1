CREATE TABLE Area
(
    AreaID      SERIAL PRIMARY KEY,
    RegName     VARCHAR,
    AreaName    VARCHAR,
    TerName     VARCHAR,
    TerTypeName VARCHAR DEFAULT NULL,
    CONSTRAINT area_unique UNIQUE (RegName, AreaName, TerName)
);