CREATE TABLE Subject(OUTID varchar,
    SchoolID int,
    Test       varchar(512),
    TestStatus varchar(256),
    Ball100    float,
    Ball12     int,
    Ball       int,
    DPALevel   varchar(128) DEFAULT NULL,
    AdaptScale int DEFAULT NULL,
    PRIMARY KEY (OUTID, Test),
    CONSTRAINT fk_student FOREIGN KEY (OUTID) REFERENCES StudentTable (OUTID),
    CONSTRAINT fk_school_school FOREIGN KEY (SchoolID) REFERENCES School (SchoolID));

CREATE
UNIQUE INDEX index_school ON
School ((Name IS NULL))
WHERE Name IS NULL;

CREATE
UNIQUE INDEX index_subject ON
Subject ((Test IS NULL))
WHERE Test IS NULL;