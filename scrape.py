import urllib.request, urllib.parse
from threading import Thread

class TextResponseWrapper:
	def __init__(self, response, encoding):
		self.response, self.encoding = response, encoding
	def read(self):
		return str(self.response.read(), self.encoding)
	def readline(self):
		return str(self.response.readline(), self.encoding)
	def __iter__(self):
		line = True
		while line:
			line = self.readline()
			yield line

class NastHarvester:
	def __init__(self):
		self.base_url = 'http://www.nast.at'
		self.encoding = 'UTF-8'
		self.headers  = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:9.0) Gecko/20100101 Firefox/9.0'}
		self.opener = urllib.request.build_opener()
	
	def fetch(self, page, args = None):
		if args:
			args = urllib.parse.urlencode(args).encode('ISO-8859-1')
		request = urllib.request.Request(self.base_url + page, args, self.headers)
		answer  = self.opener.open(request)
		return TextResponseWrapper(answer, self.encoding)

class ConsumerProducer(Thread):
	def __init__(self, consuming, producing):
		Thread.__init__(self)
		self.c, self.p = consuming, producing
	
	def run(self):
		while True:
			item = self.c.get()
			if item == None:
				self.c.put(None)
				self.p.put(None)
				print ("ending a thread of class %s." % self.__class__.__name__)
				return
			self.produce(item)
	
	def produce(self, item):
		""" Implement this """

class Consumer(Thread):
	def __init__(self, consuming):
		Thread.__init__(self)
		self.c = consuming
	
	def run(self):
		while True:
			item = self.c.get()
			#print ("%s taking item ->" % self.__class__.__name__, item)
			if item == None:
				self.c.put(None)
				print ("ending a thread of class %s." % self.__class__.__name__)
				return
			self.produce(item)
	
	def produce(self, item):
		"""Implement this """

class Storer(Consumer):
	def __init__(self, consuming, data):
		Consumer.__init__(self, consuming)
		self.data = data
	def produce(self, item):
		self.data.append(item)
		l = len(self.data)
		#print ("Stored: %d" % l)

class Itemizer(ConsumerProducer):
	def __init__(self, c, p, extractor):
		ConsumerProducer.__init__(self, c, p)
		self.extractor = extractor
	
	def produce(self, item):
		items = self.extractor.convert(item)
		for item in items:
			self.p.put(item)

