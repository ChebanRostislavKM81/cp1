CREATE TABLE School
(
    SchoolID   SERIAL PRIMARY KEY,
    AreaID       INT,
    Name     VARCHAR,
    TypeName VARCHAR DEFAULT NULL,
    Parent   VARCHAR DEFAULT NULL,
    CONSTRAINT  school_unique UNIQUE (NAME),
    CONSTRAINT fk_school_area
        FOREIGN KEY (AreaID)
            REFERENCES Area (AreaID));