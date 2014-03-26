import pickle
import json
import time
import nast

from queue import Queue
from datetime import datetime

from scrape import TextResponseWrapper, NastHarvester, Itemizer, Storer, ConsumerProducer

class Harvester(NastHarvester):
	def getPage(self, stelle, jahr, monat):
		url = '/charts/monatsauswertung/%s?ajax=change_monatsauswertung' % (stelle)
		args = {
		  'checkbox_bar': '',
		  'checkbox_detailed_temperature': 'Y',
		  'checkbox_line': 'Y',
		  'checkbox_rainfall': 'Y',
		  'checkbox_simple_temperature': '',
		  'checkbox_sum': 'Y',
		  'option_scale': 'dynamic_scale',
		  'select_date': datetime(jahr, monat, 1).timestamp()
		}
		time.sleep(0.5)
		data = self.fetch(url, args).read()
		try:
			data = json.loads(data)
		except ValueError:
			print("Failure for item %s" % item)
			return False
		data = data['jscall'][0]
		data = json.loads(data[data.find('((')+2:-len(').val)')])['val']
		data = data.replace("\r", '').replace('\t', '').split("\n")

		all_data = []
		snow_data = []
		for line in data:
			if line.startswith('chart_data'):
				all_data = json.loads(line[line.find('[['):-1])
			if line.startswith('days_with_snow_val'):
				snow_data = json.loads(line[line.find('(')+1:-len(').val;')])['val']
		return {'data': all_data, 'snow': snow_data, 'stelle': stelle, 'jahr': jahr, 'monat': monat}


class Extractor:
	def __init__(self):
		self.data = {}
		self.page = 0
	
	def convert(self, page):
		dataset = []
		d = 1
		snow = page['snow']
		for item in page['data'][1:]:
			label, r1, r2, summe, tmin, t7, t19, tmax, regen = item
			if r1 == 0 and r2 == 0 and summe == 0:
				d += 1
				continue
			dstr = str(d)
			dataset.append({
				'stelle': page['stelle'],
				'jahr': page['jahr'],
				'monat': page['monat'],
				'tag': d,
				'zaehlung_r1': r1,
				'zaehlung_r2': r2,
				'zaehlung_summe': summe,
				'regen': regen,
				'temp_min': tmin,
				'temp_max': tmax,
				'temp_7':   t7,
				'temp_19':  t19,
				'schnee': snow[dstr] if dstr in snow else 0,
			})
			d += 1
		
		return dataset
	
class Fetcher(ConsumerProducer):
	def __init__(self, c, p, harvester):
		ConsumerProducer.__init__(self, c, p)
		self.harvester = harvester
	def produce(self, item):
		page = harvester.getPage(item['stelle'], item['jahr'], item['monat'])
		if page:
			self.p.put(page)
			print ("fetched page", item)

datasets = Queue()
pages = Queue()
items = Queue()
data  = []

harvester = Harvester()
extractor = Extractor()

for stelle in nast.stellen:
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


		

