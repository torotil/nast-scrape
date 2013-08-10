import pickle
import json

from queue import Queue

from scrape import TextResponseWrapper, NastHarvester, Itemizer, Storer, ConsumerProducer

class Harvester(NastHarvester):
	def getPage(self, stelle, jahr, monat):
		url    = '/charts/read_chart/data_monatsauswertung.php?zid=%d&monat=%d&jahr=%d&temp=2&niederschlag=1&summe=1&richtung=1&richtung=1&skala=1' % (stelle, monat, jahr)
		data   = self.fetch(url).read()
		#args   = { 'art': 'Monatsauswertung', 'a': stelle, 'b': 2, 'c' : 1, 'monat' : monat, 'jahr' : jahr }
		#legend = self.fetch('/verkehrsdaten/Legende', args).read()
		return {'data': data, 'stelle': stelle, 'jahr': jahr, 'monat': monat}


class Extractor:
	def __init__(self):
		self.data = {}
		self.page = 0
	
	def convert(self, page):
		data = json.loads(page['data'][1:].strip())
		values = [x['values'] for x in data['elements'] if x['type'] != 'shape' and 'values' in x]
		
		if len(values) < 5 or len(values) > 6:
			#print(len(values), page)
			return []
		
		schnee = values.pop() if len(values) >= 6 else []
		
		
		dataset = []
		d = 1
		for item in [[j[i] for j in values] for i in range(len(values[0]))]:
			if len(item) != 5:
				print(item)
				continue
			r1, r2, summe, regen, temp = item
			dataset.append({
				'stelle': page['stelle'],
				'jahr': page['jahr'],
				'monat': page['monat'],
				'tag':   d,
				'zaehlung_r1': r1,
				'zaehlung_r2': r2,
				'zaehlung_summe': summe,
				'regen': regen['top'] if regen != None else 0,
				'temp_min': temp['low'],
				'temp_max': temp['high'],
				'temp_7':   temp['top'],
				'temp_19':  temp['bottom'],
				'schnee': 0,
			})
			d += 1
		
		for s in schnee:
			try:
				dataset[int(s['x'])]['schnee'] = int(s['tip'].split(' ')[1])
			except ValueError:
				pass
				#print(s['tip'])
		
		return dataset
	
class Fetcher(ConsumerProducer):
	def __init__(self, c, p, harvester):
		ConsumerProducer.__init__(self, c, p)
		self.harvester = harvester
	def produce(self, item):
		page = harvester.getPage(item['stelle'], item['jahr'], item['monat'])
		self.p.put(page)
		print ("fetched page", item)

datasets = Queue()
pages = Queue()
items = Queue()
data  = []

harvester = Harvester()
extractor = Extractor()

for stelle in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]:
	for monat in range(1, 13):
		for jahr in [2011, 2012, 2013]:
			datasets.put({'stelle': stelle, 'jahr': jahr, 'monat': monat})
datasets.put(None)
			
fetcher  = Fetcher(datasets, pages, harvester)
itemizer = Itemizer(pages, items, extractor)
storer   = Storer(items, data)

fetcher.start()
itemizer.start()
storer.start()

fetcher.join()
itemizer.join()
storer.join()

pickle.dump(data, open('tagesdaten.pickle', 'wb'))


		

