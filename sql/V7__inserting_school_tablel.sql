INSERT INTO School (AreaID,
                    Name,
                    TypeName,
                    Parent)
SELECT DISTINCT (SELECT AreaID
                 FROM Area
                 WHERE StartTable.EORegName = Area.RegName
                   AND StartTable.EOAreaName = Area.AreaName
                   AND StartTable.EOTerName = Area.TerName) as id,
                StartTable.EOName,
                StartTable.EOTypeName,
                StartTable.EOParent
FROM StartTable ON CONFLICT DO NOTHING;

INSERT INTO School (AreaID,
                    Name)
SELECT DISTINCT (SELECT AreaID
                 FROM Area
                 WHERE StartTable.UkrPTRegName = Area.RegName
                   AND StartTable.UkrPTAreaName = Area.AreaName
                   AND StartTable.UkrPTTerName = Area.TerName) as id,
                StartTable.UkrPTName
FROM StartTable ON CONFLICT DO NOTHING;

INSERT INTO School (AreaID,
                    Name)
SELECT DISTINCT (SELECT AreaID
                 FROM Area
                 WHERE StartTable.histPTRegName = Area.RegName
                   AND StartTable.histPTAreaName = Area.AreaName
                   AND StartTable.histPTTerName = Area.TerName) as id,
                StartTable.histPTName
FROM StartTable ON CONFLICT DO NOTHING;

INSERT INTO School (AreaID,
                    Name)
SELECT DISTINCT (SELECT AreaID
                 FROM Area
                 WHERE StartTable.mathPTRegName = Area.RegName
                   AND StartTable.mathPTAreaName = Area.AreaName
                   AND StartTable.mathPTTerName = Area.TerName) as id,
                StartTable.mathPTName
FROM StartTable ON CONFLICT DO NOTHING;

INSERT INTO School (AreaID,
                    Name)
SELECT DISTINCT (SELECT AreaID
                 FROM Area
                 WHERE StartTable.physPTRegName = Area.RegName
                   AND StartTable.physPTAreaName = Area.AreaName
                   AND StartTable.physPTTerName = Area.TerName) as id,
                StartTable.physPTName
FROM StartTable ON CONFLICT DO NOTHING;

INSERT INTO School (AreaID,
                    Name)
SELECT DISTINCT (SELECT AreaID
                 FROM Area
                 WHERE StartTable.chemPTRegName = Area.RegName
                   AND StartTable.chemPTAreaName = Area.AreaName
                   AND StartTable.chemPTTerName = Area.TerName) as id,
                StartTable.chemPTName
FROM StartTable ON CONFLICT DO NOTHING;

INSERT INTO School (AreaID,
                    Name)
SELECT DISTINCT (SELECT AreaID
                 FROM Area
                 WHERE StartTable.engPTRegName = Area.RegName
                   AND StartTable.engPTAreaName = Area.AreaName
                   AND StartTable.engPTTerName = Area.TerName) as id,
                StartTable.engPTName
FROM StartTable ON CONFLICT DO NOTHING;

INSERT INTO School (AreaID,
                    Name)
SELECT DISTINCT (SELECT AreaID
                 FROM Area
                 WHERE StartTable.fraPTRegName = Area.RegName
                   AND StartTable.fraPTAreaName = Area.AreaName
                   AND StartTable.fraPTTerName = Area.TerName) as id,
                StartTable.fraPTName
FROM StartTable ON CONFLICT DO NOTHING;

INSERT INTO School (AreaID,
                    Name)
SELECT DISTINCT (SELECT AreaID
                 FROM Area
                 WHERE StartTable.deuPTRegName = Area.RegName
                   AND StartTable.deuPTAreaName = Area.AreaName
                   AND StartTable.deuPTTerName = Area.TerName) as id,
                StartTable.deuPTName
FROM StartTable ON CONFLICT DO NOTHING;

INSERT INTO School (AreaID,
                    Name)
SELECT DISTINCT (SELECT AreaID
                 FROM Area
                 WHERE StartTable.spaPTRegName = Area.RegName
                   AND StartTable.spaPTAreaName = Area.AreaName
                   AND StartTable.spaPTTerName = Area.TerName) as id,
                StartTable.spaPTName
FROM StartTable ON CONFLICT DO NOTHING;