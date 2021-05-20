INSERT INTO Subject(OUTID,
                  SchoolID,
                  Test,
                  TestStatus,
                  Ball100,
                  Ball12,
                  Ball,
                  AdaptScale)
SELECT StartTable.OUTID,
       (SELECT SchoolID from School WHERE Name = StartTable.UkrPTName) as schoolid,
       (SELECT DISTINCT UkrTest FROM StartTable WHERE UkrTest != 'null') as discipline,
       UkrTestStatus,
       UkrBall100,
       UkrBall12,
       UkrBall,
       UkrAdaptScale
FROM StartTable WHERE StartTable.UkrTest != 'null';


INSERT INTO Subject (OUTID,
                  SchoolID,
                  Test,
                  TestStatus,
                  Ball100,
                  Ball12,
                  Ball)
SELECT StartTable.OUTID,
       (SELECT SchoolID from School WHERE Name = StartTable.histPTName) as schoolid,
       (SELECT DISTINCT histTest FROM StartTable WHERE histTest != 'null') as discipline,
       histTestStatus,
       histBall100,
       histBall12,
       histBall

FROM StartTable WHERE StartTable.histTest != 'null';

INSERT INTO Subject (OUTID,
                  SchoolID,
                  Test,
                  TestStatus,
                  Ball100,
                  Ball12,
                  Ball)
SELECT StartTable.OUTID,
       (SELECT SchoolID from School WHERE Name = StartTable.mathPTName) as schoolid,
       (SELECT DISTINCT mathTest FROM StartTable WHERE mathTest != 'null') as discipline,
       mathTestStatus,
       mathBall100,
       mathBall12,
       mathBall

FROM StartTable WHERE StartTable.mathTest != 'null';

INSERT INTO Subject (OUTID,
                  SchoolID,
                  Test,
                  TestStatus,
                  Ball100,
                  Ball12,
                  Ball)
SELECT StartTable.OUTID,
       (SELECT SchoolID from School WHERE Name = StartTable.physPTName) as schoolid,
       (SELECT DISTINCT physTest FROM StartTable WHERE physTest != 'null') as discipline,
       physTestStatus,
       physBall100,
       physBall12,
       physBall

FROM StartTable WHERE StartTable.physTest != 'null';

INSERT INTO Subject (OUTID,
                  SchoolID,
                  Test,
                  TestStatus,
                  Ball100,
                  Ball12,
                  Ball)
SELECT StartTable.OUTID,
       (SELECT SchoolID from School WHERE Name = StartTable.chemPTName) as schoolid,
       (SELECT DISTINCT chemTest FROM StartTable WHERE chemTest != 'null') as discipline,
       chemTestStatus,
       chemBall100,
       chemBall12,
       chemBall

FROM StartTable WHERE StartTable.chemTest != 'null';
INSERT INTO Subject (OUTID,
                  SchoolID,
                  Test,
                  TestStatus,
                  Ball100,
                  Ball12,
                  Ball)
SELECT StartTable.OUTID,
       (SELECT SchoolID from School WHERE Name = StartTable.bioPTName) as schoolid,
       (SELECT DISTINCT bioTest FROM StartTable WHERE bioTest != 'null') as discipline,
       bioTestStatus,
       bioBall100,
       bioBall12,
       bioBall

FROM StartTable WHERE StartTable.bioTest != 'null';
INSERT INTO Subject (OUTID,
                  SchoolID,
                  Test,
                  TestStatus,
                  Ball100,
                  Ball12,
                  Ball)
SELECT StartTable.OUTID,
       (SELECT SchoolID from School WHERE Name = StartTable.geoPTName) as schoolid,
       (SELECT DISTINCT geoTest FROM StartTable WHERE geoTest != 'null') as discipline,
       geoTestStatus,
       geoBall100,
       geoBall12,
       geoBall

FROM StartTable WHERE StartTable.geoTest != 'null';
INSERT INTO Subject (OUTID,
                  SchoolID,
                  Test,
                  TestStatus,
                  Ball100,
                  Ball12,
                  Ball,
                  DPALevel)
SELECT StartTable.OUTID,
       (SELECT SchoolID from School WHERE Name = StartTable.engPTName) as schoolid,
       (SELECT DISTINCT engTest FROM StartTable WHERE engTest != 'null') as discipline,
       engTestStatus,
       engBall100,
       engBall12,
       engBall,
       engDPALevel

FROM StartTable WHERE StartTable.engTest != 'null';
INSERT INTO Subject (OUTID,
                  SchoolID,
                  Test,
                  TestStatus,
                  Ball100,
                  Ball12,
                  Ball,
                  DPALevel)
SELECT StartTable.OUTID,
       (SELECT SchoolID from School WHERE Name = StartTable.fraPTName) as schoolid,
       (SELECT DISTINCT fraTest FROM StartTable WHERE fraTest != 'null') as discipline,
       fraTestStatus,
       fraBall100,
       fraBall12,
       fraBall,
       fraDPALevel

FROM StartTable WHERE StartTable.fraTest != 'null';
INSERT INTO Subject (OUTID,
                  SchoolID,
                  Test,
                  TestStatus,
                  Ball100,
                  Ball12,
                  Ball,
                  DPALevel)
SELECT StartTable.OUTID,
       (SELECT SchoolID from School WHERE Name = StartTable.deuPTName) as schoolid,
       (SELECT DISTINCT deuTest FROM StartTable WHERE deuTest != 'null') as discipline,
       deuTestStatus,
       deuBall100,
       deuBall12,
       deuBall,
       deuDPALevel

FROM StartTable WHERE StartTable.deuTest != 'null';
INSERT INTO Subject (OUTID,
                  SchoolID,
                  Test,
                  TestStatus,
                  Ball100,
                  Ball12,
                  Ball,
                  DPALevel)
SELECT StartTable.OUTID,
       (SELECT SchoolID from School WHERE Name = StartTable.spaPTName) as schoolid,
       (SELECT DISTINCT spaTest FROM StartTable WHERE spaTest != 'null') as discipline,
       spaTestStatus,
       spaBall100,
       spaBall12,
       spaBall,
       spaDPALevel

FROM StartTable WHERE StartTable.spaTest != 'null';