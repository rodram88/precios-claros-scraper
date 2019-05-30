#-*- coding: UTF-8 -*-

import gevent.monkey
gevent.monkey.patch_all()

import datetime
import grequests
import time
import ujson
import math
import os
import unidecode
import pandas as pd
from pandas.errors import EmptyDataError
import sqlite3
from sqlite3 import IntegrityError
from math import ceil

banderasInputFile = 'input_banderas.csv'
provinciasInputFile = 'input_provincias.csv'
comerciosInputFile =  'input_comercios.csv'
dataBaseName = 'precios_claros_sqlite.db'

curDate = int(datetime.datetime.today().strftime('%Y%m%d'))
curDateProductWindow = int((datetime.datetime.today() - datetime.timedelta(days=2)).strftime('%Y%m%d'))

# Get commerce data: id, name, address, location
def getComercios(cursor):
	concurrentsComercios = 20
	comercios = []
	comerciosUrl = 'https://d3e6htiiul5ek9.cloudfront.net/dev/sucursales'

	# Get basic data to construct urls
	data = getJsonData(comerciosUrl + '?limit=0')
	cantSucursales = data['total'] # q pages
	maxLimit = data['maxLimitPermitido']
	cantPages = int(math.ceil(cantSucursales / maxLimit))

	urls = []

	# create all the unique urls to query
	for x in range(1, cantPages + 1):
		urls.append(comerciosUrl + '?offset=' + str((x - 1) * maxLimit) + '&limit=' + str(maxLimit))

	# Create a set of unsent requests
	rs = (grequests.get(u, stream = False, timeout = 20, headers = {'User-Agent': 'Mozilla/5.0'}) for u in urls)

	# Send them all at the same time:
	responses = grequests.imap(rs, size = concurrentsComercios)
	
	for response in responses:

		# If status <> 200: Return false to force restart
		if not checkStatus(response):
			return False

		data = ujson.loads(response.content)
		comercios = comercios + data['sucursales']
		response.close()

	# Initialize the counters
	counterPositive, counterNeutral, counterNegative = 0, 0, 0

	# Parse and load commerces data in empty list
	for comercio in comercios:

		comercio_id = comercio['id']

		# Check if current comercio already loaded in database
		cursor.execute('SELECT DISTINCT comercio_id FROM comercios where comercio_id=?',(comercio_id,))
		comercioLoaded = False if cursor.fetchone() == None else True

		# Convert output to tuple to load in database
		if not comercioLoaded:
			insertData = (
				comercio_id,
				textFormatter(comercio['comercioRazonSocial']),
				textFormatter(comercio['banderaDescripcion']),
				textFormatter(comercio['direccion']),
				textFormatter(comercio['localidad']),
				textFormatter(comercio['provincia']),
				textFormatter(comercio['sucursalTipo']),
				textFormatter(comercio['sucursalNombre']),
				comercio['lat'][0:13],
				comercio['lng'][0:13],
				curDate,
				curDate,
				-1)

			# Write the data in the database
			cursor.execute('INSERT INTO comercios values(?,?,?,?,?,?,?,?,?,?,?,?,?)', insertData)
			counterPositive += 1
		else:
			cursor.execute('UPDATE comercios SET fecha_ultima_activa = ? WHERE comercio_id = ?',(curDate,comercio_id))
			counterNeutral += 1
	
	# Print summary
	print('   ',counterPositive, 'nuevos comercios encontrados.')
	print('   ',counterNeutral, 'comercios ya guardados siguen activos.')

	return True

# Get all the products for the commerece requested
def getProductos(cursor, comercio_id):
	concurrentsProductos = 10
	articulos = []
	urls = []

	# scrape the data
	productosUrl = 'https://d3e6htiiul5ek9.cloudfront.net/dev/productos?id_sucursal=' + comercio_id

	#generar total y maxLimitPermitido con 1 solo request en offset 0		
	data = getJsonData(productosUrl)
	totalProductos, maxPermitido = data['total'], data['maxLimitPermitido']

	cantPages = int(math.ceil(totalProductos / maxPermitido))

	for x in range(1, cantPages + 1):
		urls.append(productosUrl + '&offset=' + str((x - 1) * maxPermitido) + '&limit=' + str(maxPermitido))

	rs = (grequests.get(u, stream = False, timeout = 20, headers = {'User-Agent': 'Mozilla/5.0'}) for u in urls)
	responses = grequests.imap(rs, size = concurrentsProductos)

	# Append all responses in a list
	for response in responses:

		# If any status <> 200: Return false to force restart
		if not checkStatus(response):
			return False
		
		data = ujson.loads(response.content)
		articulos = articulos + data['productos']

		# Convert output to tuple to load in database
		for producto in data['productos']:

			# Get EAN data, to insert in eans table
			eanData = (str(producto['id']),
				textFormatter(producto['nombre']),
				str(producto['presentacion']),
				textFormatter(producto['marca']).title())

			# Fetch ID for the current ean
			eanID = insertEAN(cursor, eanData)

			# Delete old data and insert new values
			cursor.execute('DELETE FROM productos WHERE comercio_id = ?',(comercio_id,))
			cursor.execute('INSERT INTO productos values(?,?)', (comercio_id, eanID))

			# Print progress
			print('   -', articulos.index(producto) + 1, 'codigos EAN para descargar', end='\r')

		response.close()

	return True

# Use data generated in mainProductos to get more details (promos and availability)
def getPromos(cursor, comercio_id):
	concurrentsPromos = 10
	eanPerRequest = 100
	qResultsCounter = 0
	qPromosCounter = 0

	#List of EANs available in the commerce
	eanTuples = cursor.execute('SELECT DISTINCT b.ean FROM productos a JOIN eans b ON a.ean_id = b.id WHERE a.comercio_id = ?',(comercio_id,)).fetchall()

	eanList = []
	for ean in eanTuples:
		eanList.append(ean[0])

	# Prepare data to create URLs
	eanListLen = len(eanList)
	numRequests = ceil(eanListLen/eanPerRequest)

	# Construct URLs
	urls = []	
	for r in range(numRequests):

		infLimit = 0 + (r * eanPerRequest)
		supLimit = min((r + 1) * eanPerRequest, eanListLen + 1)
		urlPartEan = ''

		# Construct List of EANS for the current limits and number of requests
		for ean in eanList[infLimit:supLimit]:
			urlPartEan = urlPartEan + ean + ','

		# Add the generated URL to the URL list
		urls.append('https://d3e6htiiul5ek9.cloudfront.net/dev/comparativa?array_sucursales=' + comercio_id + '&array_productos=' + urlPartEan[:-1])

	# Create list of requests
	rs = (grequests.get(u, stream = False, timeout = 20, headers = {'User-Agent': 'Mozilla/5.0'}) for u in urls)
	responses = grequests.imap(rs, size = concurrentsPromos)

	# Read the responses
	for response in responses:

		# If any status <> 200: Return false to force restart
		if not checkStatus(response):
			return False
		
		# Parse response as JSON
		data = ujson.loads(response.content)

		# Check data is updated
		if not data['sucursales'][0]['actualizadoHoy']:
			continue			

		# JSON list of products for current response
		productos = data['sucursales'][0]['productos']

		for producto in productos:

			# If product exists, scrape the data
			if 'message' in producto:
					continue

			# Prepare insert data
			precioLista =  producto['precioLista']

			precioPromo1 =  producto['promo1']['precio']
			precioPromo1 = None if precioPromo1 == '' else precioPromo1

			descPromo1 = textFormatter(producto['promo1']['descripcion'])
			descPromo1Id = insertPromo(cursor, descPromo1) # Get the id for the promo 1

			precioPromo2 =  producto['promo2']['precio']
			precioPromo2 = None if precioPromo2 == '' else precioPromo2

			descPromo2 = textFormatter(producto['promo2']['descripcion'])
			descPromo2Id = insertPromo(cursor, descPromo2) # Get the id for the promo 2

			ean_id = insertEAN (cursor, (producto['id_string'],'','',''))

			insertData = (comercio_id, ean_id, precioLista, precioPromo1, descPromo1Id, precioPromo2, descPromo2Id, curDate)

			#Insert values
			try:
				cursor.execute('INSERT INTO productos_promo values(?,?,?,?,?,?,?,?)', insertData)
			except IntegrityError: # Record already exists
				continue

			qResultsCounter += 1
			if descPromo1Id != None or descPromo2Id != None:
				qPromosCounter += 1

			# Print progress
			print('   -', qResultsCounter, 'disponibles y', qPromosCounter, 'promos', end='\r')

		response.close()

	return True

# Generate list of commerces to scrape
def loadInputData(file):

	# Create empty file if doesn't exist 
	with open(file, "a") as my_empty_csv:
		pass

	# Get data and return list
	try:
		df = pd.read_csv(file, header=None, encoding="UTF-8", quotechar='"')
		return df[0].tolist()
	except EmptyDataError:
		print('El archivo', file, 'está vacio')
		return []

# Text Stripper and formatter
def textFormatter(value):
	if type(value) == str:
		return (unidecode.unidecode(value)).strip()
	elif value == None:
		return ''
	else:
		return value

# Get json data from a single url.
def getJsonData(url):
	errCounter = 0

	while True:
		response = grequests.map([grequests.get(url, stream = False, timeout = 20, headers = {'User-Agent': 'Mozilla/5.0'})])[0]

		# Break if status = 200
		if checkStatus(response):
			break
		else:
			errCounter += 1
			time.sleep(10)
			if errCounter == 10:
				print('Demasiados errores en getJsonData para la respuesta', response.content)
				exit()

	return ujson.loads(response.content)

# Check if response status = 200, else give error and terminate program
def checkStatus(req):
	if req.status_code != 200:
		print('     La API devolvió error', req.status_code,'| Cambiar IP antes de continuar. Esperando 1 minuto...')
		print('     Visitar la url del error para confirmar la causa:', req.url)
		time.sleep(60)
		return False
	else:
		return True

# Creates empty tables in databse of nonexistent 
def dataBaseInitializer(c):

	# Create sucursal table
	c.execute(''' CREATE TABLE IF NOT EXISTS comercios ( 
		comercio_id text PRIMARY KEY NOT NULL, 
		razon_social text, 
		bandera text, 
		direccion text, 
		localidad text, 
		provincia text, 
		tipo text,
		nombre text, 
		lat real, 
		lon real,
		fecha_carga integer,
		fecha_ultima_activa integer,
		fecha_scrape_prd integer) 
		''')

	c.execute('CREATE UNIQUE INDEX IF NOT EXISTS comercios_idx ON comercios (comercio_id ASC)')

	# Create productos table
	c.execute(''' CREATE TABLE IF NOT EXISTS productos ( 
		comercio_id text NOT NULL, 
		ean_id integer NOT NULL) 
		''')

	c.execute('CREATE INDEX IF NOT EXISTS productos_idx ON productos (comercio_id ASC)')

	c.execute('''CREATE TABLE IF NOT EXISTS eans (
		id integer PRIMARY KEY AUTOINCREMENT NOT NULL, 
		ean text NOT NULL, 
		desc text, 
		presentacion text, 
		marca text)
		''')

	c.execute('CREATE UNIQUE INDEX IF NOT EXISTS eans_ean_idx on eans (ean ASC)')
	c.execute('CREATE UNIQUE INDEX IF NOT EXISTS eans_id_idx on eans (id ASC)')

	c.execute('''CREATE TABLE IF NOT EXISTS productos_promo (
		comercio_id text NOT NULL, 
		ean_id integer NOT NULL, 
		precio_lista real NOT NULL, 
		promo_1_precio real, 
		promo_1_desc_id integer, 
		promo_2_precio real, 
		promo_2_desc_id integer, 
		fecha_carga integer NOT NULL)
		''')
	
	c.execute('CREATE UNIQUE INDEX IF NOT EXISTS productos_promo_idx on productos_promo (comercio_id ASC, ean_id ASC, fecha_carga ASC)')

	c.execute('''CREATE TABLE IF NOT EXISTS promos (
		id integer PRIMARY KEY AUTOINCREMENT NOT NULL, 
		desc text NOT NULL)
		''')

	c.execute('CREATE UNIQUE INDEX IF NOT EXISTS promos_desc_idx on promos (desc ASC)')
	c.execute('CREATE UNIQUE INDEX IF NOT EXISTS promos_id_idx on promos (id ASC)')

# Insert Ean in table if not exists. Returns the ID
def insertEAN (cursor, data):
	# data should be a tuple (ean, desc, presentacion, marca)
	# Insert the data. will fail if ean exists due to ean unique index constraint
	ean = data[0]
	try:
		cursor.execute('insert into eans (ean, desc, presentacion, marca) values (?, ?, ?, ?)', data)
	except IntegrityError: # Already exists 
		pass

	# Get the index
	cursor.execute('SELECT id FROM eans where ean = ?',(ean,))

	return cursor.fetchone()[0]

# Insert Promo Description in table if not exists. Returns the ID
def insertPromo(cursor, promo_desc):
	if promo_desc == '':
		return None

	try:
		cursor.execute('INSERT INTO promos (desc) VALUES (?)',(promo_desc,))
	except IntegrityError:
		pass

	return cursor.execute('SELECT id FROM promos WHERE desc = ?',(promo_desc,)).fetchone()[0]

# Get current IP requesting from ipify's api
def getIP():
	try:
		# grequests code for single URL
		return grequests.map([grequests.get('https://api.ipify.org')])[0].content
	except:
		print('Problemas obteniendo IP, visitar https://api.ipify.org para entender mejor el problema')
		return ''

# Main program
def main(scrapeComercios = True, scrapeProductos = True, scrapePromos = True):
	# List of commerces to scrape
	comerciosInput = loadInputData(comerciosInputFile)

	# Record starting time
	startTime = time.time()
	
	# Import scraping conditions
	provinciasInput = loadInputData(provinciasInputFile)
	banderasInput = loadInputData(banderasInputFile)

	# Start Database and create template tables if doesn't exist
	connection = sqlite3.connect(dataBaseName)
	c = connection.cursor()
	dataBaseInitializer(c)

	# Get all the commerces
	print (' > Descargando comercios...')

	errCounter = 0
	while len(comerciosInput) == 0 or scrapeComercios:
		try:
			# Record the IP at the begining 
			startIp = getIP()

			# Scrape the api using "sucursales" method: will return all stores with informed EANs
			# Function returns True or False for "Completed Status"
			if not getComercios(c):
				raise Exception

			# Restart if IP changed during download
			if startIp != getIP():
				print(' ')
				print('Se detectó un cambio en la IP. Descargando datos que se puderion perder')
				continue

			# Commit changes and break the loop
			connection.commit()	
			break

		except KeyboardInterrupt:
			exit()
		except Exception:
			print('Error', errCounter + 1, 'ejecutando getComercios. Reintentando...')
			raise Exception
			errCounter += 1
			if errCounter == 5:
				exit()

	# Generate list of commerces to scrape according to input parameters
	comerciosInputFull = c.execute('''select comercio_id, provincia, bandera from comercios''').fetchall()

	# Fill the list of commerces if comerciosInput is empty, using the conditions in input_provincias.csv and input_banderas.csv
	if len(comerciosInput)==0:
		for comercioFull in comerciosInputFull:
			# Fill the list with data meeting the conditions
			if comercioFull[1] in provinciasInput and comercioFull[2] in banderasInput:
				comerciosInput.append(comercioFull[0])
		print('Se generó la lista de comercios usando los parametros de input_provincias.csv + input_banderas.csv')
	else:
		print('La lista de comercios proviene de input_comercios.csv y no se usan los parametros de input_provincias.csv ni input_banderas.csv')

	for comercio in comerciosInput:

		print ('   ('+str(comerciosInput.index(comercio) + 1) + '/' + str(len(comerciosInput)) + ')', comercio)

		# Check if this ID wasn't already scraped today
		comercioLastExecuted = c.execute('SELECT fecha_scrape_prd from comercios WHERE comercio_id = ?',(comercio,)).fetchone()[0]
		if comercioLastExecuted >= curDateProductWindow:
			continue

		# Check if this comercio_id has EANS previously downloaded
		eansInComercio = c.execute('SELECT count(*) from productos WHERE comercio_id = ?',(comercio,)).fetchone()[0]

		errCounter = 0

		# check if conditions to scrape the list of eans for the commerce are met
		while (scrapeProductos and scrapePromos) or (not scrapeProductos and scrapePromos and eansInComercio < 100):
			try:
				# Record the IP at the begining 
				startIp = getIP()

				# Scrape the api using "productos" method: will return all informed prices for the store, regardles availability
				# Function returns True or False for "Completed Status"
				if not getProductos(c, comercio):
					raise Exception

				# Restart if IP changed during download
				if startIp !=  getIP():
					print(' ')
					print('Se detectó un cambio en la IP. Descargando datos que se puderion perder')
					continue

				break
			except KeyboardInterrupt:
				exit()
			except Exception:
				print('Error', errCounter + 1,'ejecutando getProductos para el comercio_id = ', comercio, '. Reintentando...')
				raise Exception
				errCounter += 1
				if errCounter == 5:
					exit()

		print('')

		errCounter = 0
		while scrapePromos:
			try:
				# Record the IP at the begining 
				startIp = getIP()
				
				# Scrape the api using "Comparativa" method. will return available products and promo prices
				# Function returns True or False for "Completed Status"
				if not getPromos(c, comercio):
					raise Exception

				# Restart if IP changed during download
				if startIp !=  getIP():
					print(' ')
					print('Se detectó un cambio en la IP. Descargando datos que se puderion perder')
					continue

				print('')
				break
			except KeyboardInterrupt:
				exit()
			except Exception:
				print('Error', errCounter + 1,'ejecutando getPromos para el comercio_id = ', comercio, '. Reintentando...')
				raise Exception
				errCounter += 1
				if errCounter == 5:
					exit()
			
		# Update comercios table, to specify that this id was scraped today
		c.execute('UPDATE comercios SET fecha_scrape_prd = ? WHERE comercio_id = ?',(curDate,comercio))
		connection.commit()	

	# Close the cursor
	c.close()

	# Print elapsed time
	print('Descarga completada en',  round((time.time() - startTime)/60,1), 'minutos.')
