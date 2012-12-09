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
	jahr=2012 AND monat=10 AND tag_typ=1;

-- Kontrolle: aktueller Quartalsvergleich
SELECT basis, jahr, quartal, tag_typ, sum(dtv_rel_monat)/count(dtv_rel_monat) AS rel_monat, sum(dtv_rel_tag)/count(dtv_rel_tag) AS rel_tag
FROM nast_quartalsentwicklung
GROUP BY basis, jahr, quartal, tag_typ
ORDER BY basis, jahr, quartal, tag_typ;

-- Schätzung Jahresentwicklung. (Stimmt auch für Jahre, bei denen Daten zu einzelnen Monaten fehlen - insbesondere das aktuelle Jahr)
SELECT basis, jahr, tag_typ, sum(dtv_rel_monat)/count(dtv_rel_monat), sum(dtv_rel_tag)/count(dtv_rel_tag)
FROM nast_jahresentwicklung
GROUP BY basis, jahr, tag_typ
ORDER BY basis, jahr, tag_typ;

--- CSV-Export
-- mysql -u root -p nast -B -e "select year(datum) as jahr, month(datum) as monat, day(datum) as tag, stelle_id, dtv_r1, dtv_r2, dtv_summe, typ as tag_typ, wochentag, nast_witterung.* from nast_tagesdtv inner join nast_witterung using(datum) inner join tage using(datum);" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > nast-daten.csv