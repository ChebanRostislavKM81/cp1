INSERT INTO StudentTable (OUTID,
                     AreaID,
                     SchoolID,
                     Birth,
                     SexTypeName,
                     RegTypeName,
                     ClassProfileName,
                     ClassLangName
)
SELECT OUTID,
        (SELECT AreaID
        FROM Area
        WHERE StartTable.RegTypeName = Area.RegName
          AND StartTable.AreaName = Area.AreaName
          AND StartTable.TerName = Area.TerName) as areaid,
       (SELECT SchoolID
        FROM School
        WHERE StartTable.EOName = School.Name)   as schoolid,
       Birth,
       SexTypeName,
       RegTypeName,
       ClassProfileName,
       ClassLangName

FROM StartTable;
