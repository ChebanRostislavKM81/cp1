CREATE TABLE IF NOT EXISTS StudentTable(OUTID varchar PRIMARY KEY,
    AreaID int,
    SchoolID int,
    Birth int,
    SEXTYPENAME varchar(10),
    REGTYPENAME varchar(100),
    AREANAME varchar(100),
    TERNAME varchar(100),
    ClassProfileNAME varchar(500),
    ClassLangName varchar(500),
    Year int,
    CONSTRAINT fk_student_area
        FOREIGN KEY (AreaID)
            REFERENCES Area (AreaID),

    CONSTRAINT fk_student_school
        FOREIGN KEY (SchoolID)
            REFERENCES School (SchoolID));