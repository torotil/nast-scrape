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
	typ ENUM('mo-fr', 'sa', 'so&feiertag')
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
	datum DATE NOT NULL,
	stelle_id TINYINT,
	tag_typ ENUM('mo-fr', 'sa', 'so&feiertag'),
	dtv INT,
	PRIMARY KEY (datum, stelle_id, tag_typ)
);""")

data = pickle.load(open('monatsdaten.pickle', 'rb'))
for d in data:
	datum = date(d['jahr'], d['monat'], 1)
	print("INSERT INTO nast_monatsdtv VALUES ('%s', %d, %d, %d);"
	       % (datum, d['stelle'],d['tag_typ']+1, d['dtv']))

#print('UPDATE nast_monatsdtv SET datum=CONCAT('2012-', MONTH(datum)+5, '-1') WHERE stelle_id = 9 AND YEAR(datum)=2012;');


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

DROP TABLE IF EXISTS nast_monatsvergleich;
CREATE TABLE nast_monatsvergleich AS
SELECT a.*, b.dtv AS dtv_vorjahr, c.dtv AS dtv_2002
FROM
	nast_monatsdtv a
	-- vorjahreswert
	LEFT OUTER JOIN nast_monatsdtv b
		ON a.stelle_id=b.stelle_id AND a.tag_typ=b.tag_typ AND MONTH(a.datum)=MONTH(b.datum) AND YEAR(a.datum)-1=YEAR(b.datum)
	-- wert 2002
	LEFT OUTER JOIN nast_monatsdtv c
		ON a.stelle_id=c.stelle_id AND a.tag_typ=c.tag_typ AND MONTH(a.datum)=MONTH(c.datum) AND YEAR(c.datum)=2002;

-- Erhöhe Zähldaten für Opernring Innen um die für Außen.
UPDATE
	nast_monatsvergleich a
	INNER JOIN nast_monatsvergleich b ON a.datum=b.datum AND a.tag_typ=b.tag_typ
SET
	a.dtv=a.dtv+b.dtv
WHERE a.stelle_id=2 AND b.stelle_id=8 AND YEAR(a.datum)=2012;


CREATE OR REPLACE VIEW tage_typpromonat AS
SELECT year(datum) AS jahr, month(datum) AS monat,
	COUNT(IF(typ=1,1,NULL)) AS mo_fr,
	COUNT(IF(typ=2,1,NULL)) AS sa,
	COUNT(IF(typ=3,1,NULL)) AS so_feiertag,
	COUNT(1) AS gesamt
FROM tage GROUP BY jahr, monat;

-- Bis 2010 wurden Samstage als Wochentage gezählt (jetzt in der Kategorie Mo-Fr). Korrektur für den Jahresvergleich
UPDATE
	nast_monatsvergleich v
	INNER JOIN tage_typpromonat t ON t.jahr=YEAR(v.datum) AND t.monat=MONTH(v.datum)
	INNER JOIN nast_monatsvergleich sa ON v.stelle_id=sa.stelle_id AND v.datum=sa.datum AND v.tag_typ=1 AND sa.tag_typ=2
SET
	v.dtv = (v.dtv*t.mo_fr+sa.dtv*t.sa)/(t.mo_fr+t.sa)
WHERE t.jahr=2011;

CREATE OR REPLACE VIEW nast_monatsentwicklung AS
SELECT
	stelle_id, datum, tag_typ, (dtv/dtv_vorjahr-1)*100 AS dtv_zum_vorjahr, (dtv/dtv_2002-1)*100 AS dtv_zu_2002
FROM
	nast_monatsvergleich;

-- Quartalszahlen
CREATE OR REPLACE VIEW nast_quartalsdtv AS
SELECT
	year(datum) AS jahr, MONTH(datum) DIV 4 + 1 AS quartal, stelle_id, typ as tag_typ, SUM(dtv_summe)/COUNT(dtv_summe) AS dtv
FROM
	nast_tagesdtv d
	INNER JOIN tage t USING(datum)
GROUP BY jahr, quartal, stelle_id, tag_typ;

CREATE OR REPLACE VIEW nast_quartalsvergleich AS
SELECT a.*, b.dtv AS dtv_vorjahr
FROM
	nast_quartalsdtv a
	-- vorjahreswert
	LEFT OUTER JOIN nast_quartalsdtv b
		ON a.stelle_id=b.stelle_id AND a.tag_typ=b.tag_typ AND a.quartal=b.quartal AND a.jahr-1=b.jahr;

CREATE OR REPLACE VIEW nast_quartalsentwicklung AS
SELECT
	stelle_id, jahr, quartal, tag_typ, (dtv/dtv_vorjahr-1)*100 AS dtv_zum_vorjahr
FROM
	nast_quartalsvergleich;

-- Jahresdaten
CREATE OR REPLACE VIEW nast_jahresdtv AS
SELECT
	year(datum) AS jahr, stelle_id, typ as tag_typ, SUM(dtv_summe)/COUNT(dtv_summe) AS dtv
FROM
	nast_tagesdtv d
	INNER JOIN tage t USING(datum)
GROUP BY jahr, stelle_id, tag_typ;

CREATE OR REPLACE VIEW nast_jahresvergleich AS
SELECT a.*, b.dtv AS dtv_vorjahr
FROM
	nast_jahresdtv a
	-- vorjahreswert
	LEFT OUTER JOIN nast_jahresdtv b
		ON a.stelle_id=b.stelle_id AND a.tag_typ=b.tag_typ AND a.jahr-1=b.jahr;


CREATE OR REPLACE VIEW nast_jahresentwicklung AS
SELECT
	stelle_id, jahr, tag_typ, (dtv/dtv_vorjahr-1)*100 AS dtv_zum_vorjahr
FROM
	nast_jahresvergleich;
	

""")
