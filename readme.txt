https://www.preciosclaros.gob.ar/ es un programa del gobierno argentino del que participan las principales cadenas de supermercados 
de toda la región. Su función es informar a los consumidores sobre los mejores precios cercanos a su ubicación.
Cada Retail comparte con Precios Claros de forma diaria, todos los precios de un set de articulos por punto de venta.

Este programa consulta a la API de precios claros para obtener todos los precios de un listado de tienda o provincias predefinidos, 
y almacena los datos en una base sqlite. 
Dicho proceso conlleva obstaculos como:
	- la API impone límites a la cantidad de resultados a consultar por día. Esto se supera usando un servicio de VPN como ibVPN
	  que permite rotar entre un set de IPs cada cierto intervalo de tiempo. El programa contempla esto, y consulta la IP al 
	  comenzar y finalizar la descarga de datos de cada comercio. Si los datos difieren se reinicia la descarga del comercio.
	- al consultar un comercio, la API devuelve los resultados en N páginas.

--------------------------------------------------------------------------------------------------------------------------------------

Instalacion:

se requiere: 
	- Python 3.6
	- PIP
	- Virtualenv

Validar que existen con los comandos: python --version / pip --version / virtualenv --version

Crear virtualenv
	- En la carpeta root del proyecto ejecutar el comando: virtualenv nombre_venv

Solo la primera vez que se usa el venv se deben instalar las dependencias del programa, ubicadas en requirements.txt
	- una vez activado el virtual env en el root folder del proyecto usar el comando: pip install -r requirements.txt

--------------------------------------------------------------------------------------------------------------------------------------

Para ejecutar utilizar run_scraper.bat

Sobre la primer ejecucion:
	No es necesario tener los csv inptus o la base de datos creada para ejecutar por primera vez.
	Esta ejecución solo va a generar un listado de tiendas y sus atributos en la base de datos, 
	 que luego se van a usar con los filtros ingresados en los inputs

Siguientes ejecuciones:
	Los archivos input_banderas.csv, input_provincias.csv y input_comercios.csv ya 
	deben estan creados y completos con los parametros de descarga.
	Si no se completan, con la ejecucipon solo se actualizan los datos de comercios.

--------------------------------------------------------------------------------------------------------------------------------------

Almacenamiento
Toda la data se almacena en una base de datos sqlite
Tablas:
	- Comercios
		Contiene el listado de comercios y sus atributos, ademas de las fechas de carga, revision, y descarga de productos
	- EANS
		Cada nuevo ean que se descargue, se almacena en esta tabla, junto a sus atributos:
			- Descripcion
			- Presentación
			- Marca
	- Productos
		Contiene la data descargada por el metodo "Productos" de la API
		Devuelve un listado completo de productos de la tienda, y solo el precio de lista
	- Productos_Promo
		Con los EANS activos en la tienda, generado por el metodo "Productos", se los usa como input en el metodo "Comparativa"
		Esto devuelve ademas los precios promocionales y si los productos se encuentran disponibles
	- Promos
		A fin de no almacenar las descripciones en cada registro de Productos_Promo, se almacenan en esta tabla y se les	
		 asigna un ID

--------------------------------------------------------------------------------------------------------------------------------------

Input data: 
	Definen los parametros de busqueda: Todos los comercios que cumplan con las condiciones especificadas en estos
	archivos se van a descargar con el programa
	Los datos ingresados eben listarse una debajo del otro, sin quotes

	- Input Banderas:
		Ingresar lista de banderas a descargar		
	- Input Provincias:
		Ingresar lista de provincias a descargar

	Estas 2 listas funcionan en simultaneo: Se descargan Banderas de las provincias especificadas.

	Otra forma de seleccionar las tiendas, es directamente especificandolas en input_comercios.csv

	- Input Comercios:
		Si algun comercio ID se ingresó en esta lista, se descarta lo ingresado en Input Banderas e Input Provincias
		 y se descarga unicamente lo especificado en este archivo.

--------------------------------------------------------------------------------------------------------------------------------------

Provincias:

AR-A	Salta
AR-B	Buenos Aires
AR-C	Capital Federal
AR-D	San Luis
AR-E	Entre Rios
AR-F	La Rioja
AR-G	Santiago del Estero
AR-H	Chaco
AR-J	San Juan
AR-K	Catamarca
AR-L	La Pampa
AR-M	Mendoza
AR-N	Misiones
AR-P	Formosa
AR-Q	Neuquen
AR-R	Rio Negro
AR-S	Santa Fe
AR-T	Tucuman
AR-U	Chubut
AR-V	Ushuaia
AR-W	Corrientes
AR-X	Cordoba
AR-Y	Jujuy
AR-Z	Santa Cruz

--------------------------------------------------------------------------------------------------------------------------------------

Banderas: (actualizada al 04-05-2019)

Supermercados DIA
Express
Vea
La Anonima
Cooperativa Obrera Limitada de Consumo y Vivienda
COTO CICSA
Atomo
Market
Hipermercado Carrefour
Disco
Axion Energy
Changomas
DEHEZA S.A.I.C.F. e I.
Simplicity
Walmart SuperCenter
Supermercados Cordiez
Jumbo
Toledo
Supermercados Comodin
Topsy
Hipermercado Libertad
SuperCLC
Mini Libertad
Supermercados Becerra
Super Aiello
Mariano Max
Abastecimiento Mercamax
JOSIMAR SUPERMERCADOS
Mi Changomas
California Supermercados
Super MAMI
LA AGRICOLA REGIONAL COOPERATIVA LIMITADA
SUPERMERCADOS EL NENE
Maxi Comodin
Sup. Unicoop
Bomba
Super Tuti1 y Super Tuti 3
Super Tuti2 y Super Todo
ALMACENES PAMPAS
ALMACENES DE MARCA
MUCOOP CAP.SARMIENTO
LA AMISTAD
ESTACION LIMA S.A.
