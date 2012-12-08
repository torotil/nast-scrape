-- Kontrolle: Versuche aus den Tageszähldaten die Monatszählwerte zu errechnen.
SELECT *, dtv_rechnung-dtv FROM (
  SELECT
    date_format(d.datum, '%Y-%m') as monat,
    d.stelle_id,
    round(sum(d.dtv_summe)/count(d.dtv_summe)) as dtv_rechnung,
    m.tag_typ,
    m.dtv
  FROM
    nast_tagesdtv d
    INNER JOIN tage t USING(datum)
    INNER JOIN nast_monatsdtv m ON m.stelle_id = d.stelle_id AND date_format(m.datum, '%Y-%m') = date_format(d.datum, '%Y-%m') AND m.tag_typ = t.typ
  GROUP BY
    d.stelle_id, monat, t.typ
) a ORDER BY stelle_id, monat;

-- Kontrolle: Jahresvergleichswerte (der Monatsdaten) für die Argentinierstraße
SELECT
	a.datum, a.dtv, b.datum, b.dtv, (a.dtv/b.dtv-1)*100 AS entwicklung
FROM
	nast_monatsdtv a
	INNER JOIN nast_monatsdtv b ON a.stelle_id=b.stelle_id AND a.tag_typ=b.tag_typ AND YEAR(b.datum)=2002 AND MONTH(a.datum)=MONTH(b.datum)
WHERE
	a.stelle_id=7 AND a.tag_typ=1 AND YEAR(a.datum) > 2002
ORDER BY
	a.datum;

-- Kontrolle: "Aktuelle Entwicklung im Oktober"
SELECT s.name, e.*
FROM
	nast_monatsentwicklung e
	INNER JOIN nast_zaehlstellen s ON s.id=e.stelle_id
WHERE
	year(datum)=2012 AND month(datum)=10 AND tag_typ=1;

-- Kontrolle: aktueller Quartalsvergleich
SELECT
	tag_typ, sum(dtv_zum_vorjahr)/count(dtv_zum_vorjahr)
FROM
	nast_quartalsentwicklung
WHERE
	jahr=2012 AND quartal=3 GROUP BY tag_typ;

-- Entwicklung laufendes Jahr bis …:
SELECT tag_typ, sum(entwicklung)/count(entwicklung) FROM
	(SELECT stelle_id, tag_typ, (d2012.dtv/d2011.dtv-1)*100 AS entwicklung FROM
		(SELECT year(datum) AS jahr, stelle_id, typ as tag_typ, SUM(dtv_summe)/COUNT(dtv_summe) AS dtv FROM nast_tagesdtv d INNER JOIN tage t USING(datum) WHERE year(datum)=2011 AND month(datum)<=10 AND stelle_id <= 7 GROUP BY jahr, stelle_id, tag_typ) d2011
		INNER JOIN
		(SELECT year(datum) AS jahr, stelle_id, typ as tag_typ, SUM(dtv_summe)/COUNT(dtv_summe) AS dtv FROM nast_tagesdtv d INNER JOIN tage t USING(datum) WHERE year(datum)=2012 AND month(datum)<=10 AND stelle_id <= 7 GROUP BY jahr, stelle_id, tag_typ) d2012
		USING (stelle_id, tag_typ)
	) e
GROUP BY tag_typ;

-- Schätzung Jahresentwicklung 2010 -> 2011
SELECT tag_typ, sum(dtv_zum_vorjahr)/count(dtv_zum_vorjahr) AS entwicklung
FROM (
	SELECT MONTH(datum) AS monat, tag_typ, dtv_zum_vorjahr
	FROM nast_monatsentwicklung
	WHERE year(datum)=2011 AND tag_typ IN (1,3)
	GROUP BY monat, tag_typ) a
GROUP BY tag_typ;

--- CSV-Export
-- mysql -u root -p nast -B -e "select year(datum) as jahr, month(datum) as monat, day(datum) as tag, stelle_id, dtv_r1, dtv_r2, dtv_summe, typ as tag_typ, wochentag, nast_witterung.* from nast_tagesdtv inner join nast_witterung using(datum) inner join tage using(datum);" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > nast-daten.csv