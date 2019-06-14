https://www.preciosclaros.gob.ar/ es un programa del gobierno argentino del que participan las principales cadenas de supermercados 
de toda la región. Su función es informar a los consumidores sobre los mejores precios cercanos a su ubicación.
Cada Retail comparte con Precios Claros de forma diaria, todos los precios de un set de articulos por punto de venta.

Este programa consulta a la API de precios claros para obtener todos los precios de un listado de tienda o provincias predefinidos, 
y almacena los datos en una base sqlite. 

--------------------------------------------------------------------------------------------------------------------------------------

Ejemplos de los datos descargados

- Comercios y su ubicación
	- Color = Bandera
	- Tamaño = amplitud de surtido del punto de venta
![testimage](https://github.com/rodram88/precios-claros-scraper/blob/master/images/comercios.PNG?raw=true)

Analisis de esquema tarifario de bandera Carrefour
- Color de burbuja representa un esquema tarifario
	- Rojo = Precio alto
	- Amarillo = Precio Medio
	- Verde = Precio Bajo
- Tamaño = amplitud de surtido del punto de venta
![testimage](https://github.com/rodram88/precios-claros-scraper/blob/master/images/esquema_tarifario_carrefour.png?raw=true)

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
- Comercios: Contiene el listado de comercios y sus atributos, ademas de las fechas de carga, revision, y descarga de productos
- EANS: Cada nuevo ean que se descargue, se almacena en esta tabla, junto a sus atributos:
	- Descripcion
	- Presentación
	- Marca
- Productos
	- Contiene la data descargada por el metodo "Productos" de la API
	- Devuelve un listado completo de productos de la tienda, y solo el precio de lista
- Productos_Promo
	- Con los EANS activos en la tienda, generado por el metodo "Productos", se los usa como input en el metodo "Comparativa"
	- Esto devuelve ademas los precios promocionales y si los productos se encuentran disponibles
- Promos
	- A fin de no almacenar las descripciones en cada registro de Productos_Promo, se almacenan en esta tabla y se les asigna un ID

--------------------------------------------------------------------------------------------------------------------------------------

Input data: 
Definen los parametros de busqueda: Todos los comercios que cumplan con las condiciones especificadas en estos
archivos se van a descargar con el programa

Los datos ingresados eben listarse una debajo del otro, sin quotes

- Input_Banderas.csv: Ingresar lista de banderas a descargar		
- Input_Provincias.csv: Ingresar lista de provincias a descargar

Estas 2 listas funcionan en simultaneo: Se descargan Banderas de las provincias especificadas.

Otra forma de seleccionar las tiendas, es directamente especificandolas en input_comercios.csv
Esta forma omite lo ingresado en Input_Banderas.csv e Input_Provincias.csv
- Input Comercios:
	Si algun comercio ID se ingresó en esta lista, se descarta lo ingresado en Input Banderas e Input Provincias
	 y se descarga unicamente lo especificado en este archivo.

- Comercios_y_provincias_txt contiene referencias a los datos a ingresar en los csv inputs

