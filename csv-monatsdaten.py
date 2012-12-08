import csv
import pickle
import sys

data = pickle.load(open('monatsdaten.pickle', 'rb'))
writer = csv.DictWriter(sys.stdout, ['jahr','monat','stelle', 'tag_typ', 'dtv'])
writer.writeheader()
writer.writerows(data)
