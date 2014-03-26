import pickle
import json
import time
import nast

from queue import Queue

from scrape import TextResponseWrapper, NastHarvester, Itemizer, Storer, ConsumerProducer

class Harvester(NastHarvester):
	def getPage(self, stelle):
		args = {}
		for year in range(2002, 2014):
			args['year_%s' % year] = 'Y'
		for month in range(1, 13):
			args['month_%s' % month] = 'Y'
		
		url = '/charts/entwicklung/%s?ajax=change_entwicklung' % (stelle)
		time.sleep(0.5)

		ret = []
		for d in ['wt', 'sa', 'sof']:
			args['option_dtv'] = d
			data = self.fetch(url, args).read()
			try:
				data = json.loads(data)
			except ValueError:
				print("Failure for item %s" % item)
				continue
			data = data['jscall'][0]
			data = json.loads(data[data.find('((')+2:-len(').val)')])['val']
			data = data.replace("\r", '').replace('\t', '').split("\n")
			all_data = []
			for line in data:
				if line.startswith('chart_data'):
					all_data = json.loads(line[line.find('[['):-1])
			ret.append(all_data)
		return {'data': ret, 'stelle': stelle}


class Extractor:
	def convert(self, page):
		dataset = []
		for typ in range(3):
			jahre = [int(x) for x in page['data'][typ][0][1:]]
			for dtv_years in page['data'][typ][1:]:
				if dtv_years[0] == 'JAHR':
					continue
				monat = nast.monat[dtv_years[0]]
				for jahr, dtv in zip(jahre, dtv_years[1:]):
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

for stelle in nast.stellen:
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


		

