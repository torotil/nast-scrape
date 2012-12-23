from datetime import date, timedelta
from feiertage import feiertage_wien
import pickle

print("""
DROP TABLE IF EXISTS tage_feiertage;

CREATE TABLE tage_feiertage (
	datum DATE NOT NULL,
	name VARCHAR(127) NOT NULL,
	PRIMARY KEY (datum, name),
	INDEX feiertag_datum (datum)
);
""")

for year in range(2002, 2016):
	feiertage = feiertage_wien(year)
	for name, day in feiertage.items():
		print("INSERT INTO `tage_feiertage` (datum, name) VALUES ('%s', '%s');" % (day, name))

print("""
DROP TABLE IF EXISTS tage;

CREATE TABLE tage (
	datum DATE NOT NULL PRIMARY KEY,
	wochentag TINYINT,
	typ ENUM('mo-fr', 'sa', 'so&feiertag', 'werktag')
);""")

day = date(2002, 1, 1)
step = timedelta(1)
end = date(2016, 1, 1)

while day < end:
	wd  = day.weekday()
	typ = 1 if wd <= 4 else (2 if wd == 5 else 3)
	print("INSERT INTO tage (datum, wochentag, typ) VALUES ('%s', %d, %d);" % (day, wd, typ))
	day += step

print("UPDATE tage INNER JOIN tage_feiertage USING(datum) SET tage.typ=3;")

print("""
DROP TABLE IF EXISTS nast_tagesdtv;

CREATE TABLE nast_tagesdtv (
	datum  DATE NOT NULL,
	stelle_id TINYINT,
	dtv_r1 INT,
	dtv_r2 INT,
	dtv_summe INT,
	PRIMARY KEY (datum, stelle_id)
);
""")

witterung = []

data = pickle.load(open('tagesdaten.pickle', 'rb'))
for d in data:
	datum = date(int(d['jahr']), int(d['monat']), int(d['tag']))
	print("INSERT INTO nast_tagesdtv VALUES ('%s', %d, %d, %d, %d);"
	      % (datum, d['stelle'], d['zaehlung_r1'], d['zaehlung_r2'], d['zaehlung_summe']))
	if d['stelle'] == 0:
		witterung.append((datum, d['regen'], d['schnee'], d['temp_min'], d['temp_max'], d['temp_7'], d['temp_19']))

print("""
DROP TABLE IF EXISTS nast_witterung;

CREATE TABLE nast_witterung (
	datum DATE NOT NULL PRIMARY KEY,
	regen DECIMAL(5,2),
	schnee DECIMAL(5,2),
	temp_min DECIMAL(5,2),
	temp_max DECIMAL(5,2),
	temp_7 DECIMAL(5.2),
	temp_19 DECIMAL(5.2)
);

INSERT INTO nast_witterung VALUES
""")
print(",\n".join(["('%s', %s, %s, %s, %s, %s, %s)" % w for w in witterung]), ';')
del witterung

print("""
DROP TABLE IF EXISTS nast_monatsdtv;

CREATE TABLE nast_monatsdtv (
	jahr INT NOT NULL,
	monat INT NOT NULL,
	stelle_id TINYINT,
	tag_typ ENUM('mo-fr', 'sa', 'so&feiertag', 'werktag'),
	dtv INT,
	PRIMARY KEY (jahr, monat, stelle_id, tag_typ)
);""")

data = pickle.load(open('monatsdaten.pickle', 'rb'))
for d in data:
	datum = date(d['jahr'], d['monat'], 1)
	print("INSERT INTO nast_monatsdtv VALUES (%d, %d, %d, %d, %d);"
	       % (d['jahr'], d['monat'], d['stelle'],d['tag_typ']+1, d['dtv']))

print("""

DROP TABLE IF EXISTS nast_zaehlstellen;

CREATE TABLE nast_zaehlstellen (
	id TINYINT PRIMARY KEY,
	name VARCHAR(255),
	r1 VARCHAR(255),
	r2 VARCHAR(255)
);

INSERT INTO nast_zaehlstellen VALUES
(7, 'Argentinierstraße', 'Zentrum',       'Südbahnhof'),
(4, 'Donaukanal',        'Zentrum',       'Klosterneuburg'),
(6, 'Langobardenstraße', 'Stadlau',       'Aspern'),
(3, 'Lassallestraße',    'Praterstern',   'Reichsbrücke'),
(5, 'Liesingbach',       'Inzersdorf',    'Atzgersdorf'),
(0, 'Westbahnhof',       'Burggasse',     'Mariahilferstraße'),
(2, 'Opernring Innen',   'Parlament',     'Oper'),
(8, 'Opernring Außen',   'Parlament',     'Oper'),
(1, 'Wienzeile',         'Stadtauswärts', 'Zentrum'),
(9, 'Margaritensteg',    'Richtung 1',    'Richtung 2');


--- Nützliche Views und Tables

UPDATE nast_monatsdtv SET tag_typ=4 WHERE jahr<=2010 AND tag_typ=1;
DELETE FROM nast_monatsdtv WHERE jahr<=2010 AND tag_typ=2;

CREATE OR REPLACE VIEW tage_typpromonat AS
SELECT year(datum) AS jahr, month(datum) AS monat,
	COUNT(IF(typ=1,1,NULL)) AS mo_fr,
	COUNT(IF(typ=2,1,NULL)) AS sa,
	COUNT(IF(typ=3,1,NULL)) AS so_feiertag,
	COUNT(1) AS gesamt
FROM tage GROUP BY jahr, monat;

CREATE OR REPLACE VIEW tage_tagepromonatundtyp AS
SELECT year(datum) AS jahr, month(datum) AS monat, typ, count(datum) AS tage FROM tage GROUP BY typ, jahr, monat
UNION
SELECT year(datum) AS jahr, month(datum) AS monat, 'werktag' AS typ, count(datum) AS tage FROM tage WHERE typ IN(1,2) GROUP BY jahr, monat;

INSERT INTO nast_monatsdtv (jahr, monat, stelle_id, tag_typ, dtv)
SELECT d.jahr, d.monat, d.stelle_id, 4, round(sum(d.dtv*t.tage)/sum(t.tage))
FROM nast_monatsdtv d
INNER JOIN tage_tagepromonatundtyp t ON d.jahr=t.jahr AND d.monat=t.monat AND d.tag_typ=t.typ
WHERE d.jahr>=2011 AND d.tag_typ IN(1,2) GROUP BY d.jahr, d.monat, d.stelle_id;

DROP TABLE IF EXISTS nast_monatsvergleich;
CREATE TABLE nast_monatsvergleich (
	basis VARCHAR(32),
	jahr INT NOT NULL,
	monat INT NOT NULL,
	stelle_id TINYINT,
	tag_typ ENUM('mo-fr', 'sa', 'so&feiertag', 'werktag'),
	dtv INT,
	dtv_basis INT,
	PRIMARY KEY (basis, jahr, monat, stelle_id, tag_typ)
);
INSERT INTO nast_monatsvergleich
SELECT 'vorjahr' AS basis, a.*, b.dtv AS dtv_basis FROM nast_monatsdtv a INNER JOIN nast_monatsdtv b ON a.stelle_id=b.stelle_id AND a.tag_typ=b.tag_typ AND a.monat=b.monat AND a.jahr-1=b.jahr
UNION
SELECT '2002' AS basis, a.*, b.dtv AS dtv_basis FROM nast_monatsdtv a INNER JOIN nast_monatsdtv b ON a.stelle_id=b.stelle_id AND a.tag_typ=b.tag_typ AND a.monat=b.monat AND b.jahr=2002
;

-- Erhöhe Zähldaten für Opernring Innen um die für Außen.
UPDATE
	nast_monatsvergleich a
	INNER JOIN nast_monatsdtv b ON a.jahr=b.jahr AND a.monat=b.monat AND a.tag_typ=b.tag_typ
SET
	a.dtv=a.dtv+b.dtv
WHERE a.stelle_id=2 AND b.stelle_id=8 AND a.jahr=2012;


CREATE OR REPLACE VIEW nast_monatsentwicklung AS
SELECT
	basis, stelle_id, jahr, monat, tag_typ, (dtv/dtv_basis-1)*100 AS dtv_rel
FROM
	nast_monatsvergleich;

-- Quartalszahlen
CREATE OR REPLACE VIEW nast_quartalsdtv AS
SELECT d.jahr, d.monat DIV 4 + 1 AS quartal, d.stelle_id, d.tag_typ, avg(d.dtv) AS dtv_monat, 2*std(d.dtv)/sqrt(count(d.dtv)) AS twostdev, sum(d.dtv*t.tage)/sum(t.tage) AS dtv_tag
FROM nast_monatsdtv d INNER JOIN tage_tagepromonatundtyp t ON d.jahr=t.jahr AND d.monat=t.monat AND d.tag_typ=t.typ
GROUP BY d.jahr, quartal, d.tag_typ, d.stelle_id;

CREATE OR REPLACE VIEW nast_quartalsvergleich AS
SELECT d.basis, d.jahr, d.monat DIV 4 + 1 AS quartal, d.stelle_id, d.tag_typ, avg(d.dtv) AS dtv_monat, avg(d.dtv_basis) AS dtv_monat_basis, avg(d.dtv/d.dtv_basis) AS dtv_monat_rel, 2*std(d.dtv/d.dtv_basis)/sqrt(count(d.dtv)) AS dtv_monat_2stddev, sum(d.dtv*t.tage)/sum(t.tage) AS dtv_tag, sum(d.dtv_basis*u.tage)/sum(u.tage) AS dtv_tag_basis
FROM nast_monatsvergleich d
INNER JOIN tage_tagepromonatundtyp t ON d.jahr=t.jahr AND d.monat=t.monat AND d.tag_typ=t.typ
INNER JOIN tage_tagepromonatundtyp u ON d.jahr-1=u.jahr AND d.monat=u.monat AND d.tag_typ=u.typ
WHERE d.basis='vorjahr'
GROUP BY d.basis, d.jahr, quartal, d.tag_typ, d.stelle_id;

CREATE OR REPLACE VIEW nast_quartalsentwicklung AS
SELECT d.basis, d.jahr, d.monat DIV 4 + 1 AS quartal, d.tag_typ, avg(d.dtv) AS dtv_monat, avg(d.dtv_basis) AS dtv_monat_basis, avg(d.dtv/d.dtv_basis) AS dtv_monat_rel, 2*std(d.dtv/d.dtv_basis)/sqrt(count(d.dtv)) AS dtv_monat_2stddev, sum(d.dtv*t.tage)/sum(t.tage) AS dtv_tag, sum(d.dtv_basis*u.tage)/sum(u.tage) AS dtv_tag_basis
FROM nast_monatsvergleich d
INNER JOIN tage_tagepromonatundtyp t ON d.jahr=t.jahr AND d.monat=t.monat AND d.tag_typ=t.typ
INNER JOIN tage_tagepromonatundtyp u ON d.jahr-1=u.jahr AND d.monat=u.monat AND d.tag_typ=u.typ
WHERE d.basis='vorjahr'
GROUP BY d.basis, d.jahr, quartal, d.tag_typ;

-- Jahresdaten
CREATE OR REPLACE VIEW nast_jahresdtv AS
SELECT d.jahr, d.stelle_id, d.tag_typ, avg(d.dtv) AS dtv_monat, std(d.dtv)/sqrt(count(d.dtv)) as dtv_monat_2stddev, sum(d.dtv*t.tage)/sum(t.tage) AS dtv_tag
FROM nast_monatsdtv d INNER JOIN tage_tagepromonatundtyp t ON d.jahr=t.jahr AND d.monat=t.monat AND d.tag_typ=t.typ
GROUP BY d.jahr, d.tag_typ, d.stelle_id;

CREATE OR REPLACE VIEW nast_jahresvergleich AS
SELECT d.basis, d.jahr, d.stelle_id, d.tag_typ,
	avg(d.dtv) AS dtv_monat, std(d.dtv)/sqrt(count(d.dtv)) as dtv_monat_2stddev, sum(d.dtv_basis)/count(d.dtv_basis) AS dtv_monat_basis,
  sum(d.dtv*t.tage)/sum(t.tage) AS dtv_tag, sum(d.dtv_basis*u.tage)/sum(u.tage) AS dtv_tag_basis
FROM nast_monatsvergleich d
INNER JOIN tage_tagepromonatundtyp t ON d.jahr=t.jahr AND d.monat=t.monat AND d.tag_typ=t.typ
INNER JOIN tage_tagepromonatundtyp u ON d.jahr-1=u.jahr AND d.monat=u.monat AND d.tag_typ=u.typ
WHERE d.basis='vorjahr'
GROUP BY d.basis, d.jahr, d.tag_typ, d.stelle_id
UNION
SELECT d.basis, d.jahr, d.stelle_id, d.tag_typ,
	avg(d.dtv) AS dtv_monat, std(d.dtv)/sqrt(count(d.dtv)) as dtv_monat_2stddev, sum(d.dtv_basis)/count(d.dtv_basis) AS dtv_monat_basis,
  sum(d.dtv*t.tage)/sum(t.tage) AS dtv_tag, sum(d.dtv_basis*u.tage)/sum(u.tage) AS dtv_tag_basis
FROM nast_monatsvergleich d
INNER JOIN tage_tagepromonatundtyp t ON d.jahr=t.jahr AND d.monat=t.monat AND d.tag_typ=t.typ
INNER JOIN tage_tagepromonatundtyp u ON u.jahr=2002 AND d.monat=u.monat AND d.tag_typ=u.typ
WHERE d.basis='2002'
GROUP BY d.basis, d.jahr, d.tag_typ, d.stelle_id;

CREATE OR REPLACE VIEW nast_jahresentwicklung AS
SELECT d.basis, d.jahr, d.tag_typ,
	avg(d.dtv/d.dtv_basis) AS dtv_rel, std(d.dtv/d.dtv_basis)/sqrt(count(d.dtv)) as dtv_rel_2stddev
FROM nast_monatsvergleich d
WHERE d.basis='vorjahr'
GROUP BY d.basis, d.jahr, d.tag_typ
UNION
SELECT d.basis, d.jahr, d.tag_typ,
	avg(d.dtv/d.dtv_basis) AS dtv_rel, std(d.dtv/d.dtv_basis)/sqrt(count(d.dtv)) as dtv_rel_2stddev
FROM nast_monatsvergleich d
WHERE d.basis='2002'
GROUP BY d.basis, d.jahr, d.tag_typ;

""")
