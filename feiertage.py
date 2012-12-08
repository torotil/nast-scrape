from datetime import date, timedelta

def ostersonntag(jahr):
	jahrhundert = jahr // 100
	J = (3*jahrhundert + 3) // 4
	mondschaltung = 15 + J - (8*jahrhundert+13) // 25
	mondparameter = jahr % 19
	vollmond = (19*mondparameter + mondschaltung) % 30
	korrektur = (vollmond+mondparameter // 11) // 29  # immer 1 oder 0
	ostergrenze = 21 + vollmond - korrektur
	maerz_sonntag = 7 - (jahr + jahr // 4 + 2 - J) % 7
	maerzdatum = ostergrenze + 7 - (ostergrenze-maerz_sonntag) % 7
	month = (maerzdatum - 1) // 31 + 3
	day = maerzdatum % (31 + (4 - month))
	return date(jahr, month, day)

def feiertage_wien(jahr):
	os = ostersonntag(jahr) 
	return {
		'Neujahr':             date(jahr,  1,  1),
		'Heilige Drei Könige': date(jahr,  1,  6),
		#'Karfreitag':          os - timedelta( 2),
		'Ostermontag':         os + timedelta( 1),
		'Staatsfeiertag':      date(jahr,  5,  1),
		'Christi Himmelfahrt': os + timedelta(39),
		'Pfingstmontag':       os + timedelta(50),
		'Fronleichnam':        os + timedelta(60),
		'Maria Himmelfahrt':   date(jahr,  8, 15),
		'Nationalfeiertag':    date(jahr, 10, 26),
		'Allerheiligen':       date(jahr, 11,  1),
		#'Leopold':             date(jahr, 11, 15),
		'Maria Empfängnis':    date(jahr, 12,  8),
		#'Heiliger Abend':      date(jahr, 12, 24),
		'Christtag':           date(jahr, 12, 25),
		'Stefanitag':          date(jahr, 12, 26),
		#'Silvester':           date(jahr, 12, 31),
	}

def days_in_year(year):
	d = date(year, 1, 1)
	f = {v:k for k,v in feiertage_wien(year).items()}
	day = timedelta(1)
	while d.year == year:
		print(d, f[d] if d in f else None, d.weekday())
		d += day

		