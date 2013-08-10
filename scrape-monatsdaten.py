import pickle
import json

from queue import Queue

from scrape import TextResponseWrapper, NastHarvester, Itemizer, Storer, ConsumerProducer

class Harvester(NastHarvester):
	def getPage(self, stelle):
		jahre  = '-'.join([str(x) for x in range(2002,2013)])
		monate = '-'.join([str(x) for x in range(1,13)])
		url    = '/charts/read_chart/data_entwicklung.php?get_variable=%d--%s--%s--%%d' % (stelle, jahre, monate)
		data   = []
		for typ in range(3):
			# for some reason the first character in the response is corrupted
			data.append(self.fetch(url % (typ, )).read())
		return {'data': data, 'stelle': stelle}


class Extractor:
	def convert(self, page):
		dataset = []
		for typ in range(3):
			data = json.loads(page['data'][typ])
			values = [x['values'] for x in data['elements'] if 'values' in x]
			for jahr, dtv_months in zip(range(2002,2013), values):
				for monat, dtv in zip(range(1,13), dtv_months[:-1]):
					dataset.append({
						'stelle':  page['stelle'],
						'jahr':    jahr,
						'monat':   monat,
						'tag_typ': typ,
						'dtv':     dtv,
					})
		return dataset
	
class Fetcher(ConsumerProducer):
	def __init__(self, c, p, harvester):
		ConsumerProducer.__init__(self, c, p)
		self.harvester = harvester
	def produce(self, item):
		page = harvester.getPage(item['stelle'])
		self.p.put(page)
		print ("fetched page", item)

datasets = Queue()
pages = Queue()
items = Queue()
data  = []

harvester = Harvester()
extractor = Extractor()

for stelle in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]:
			datasets.put({'stelle': stelle})
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

pickle.dump(data, open('monatsdaten.pickle', 'wb'))


		

