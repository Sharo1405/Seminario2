##Archivo 1
##Reporte 1
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext("local", "first app")
spark = SparkSession(sc)
import plotly.graph_objects as go
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

text_file = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', quote='"', delimiter=',').load("C:\\Users\\sharolin\\Desktop\\entrada\\GuatemalaExportsTo.csv")
rddfiltro = text_file.rdd.map(tuple)
rddPAIS = rddfiltro.map(lambda word: (word[4],word[1]))
rddPAIS.take(5)


rddCONTEO=rddPAIS.reduceByKey(lambda a,b: a+b)
print("Conteo total -> %s" % rddCONTEO.collect())


rddORDEN = sc.parallelize(rddCONTEO.sortBy(lambda a: a[1],False).take(5))
print("Pais con mayor valor de exportaciones -> %s" % rddORDEN.collect())


rddNombres = rddORDEN.map(lambda x: (x[0]))
rddTotales = rddORDEN.map(lambda x: (x[1]))
print(rddNombres.collect())
print(rddTotales.collect())


fig = go.Figure(data=go.Bar(x=rddNombres.collect(),y=rddTotales.collect()))
fig.update_layout(title_text='Pais con mayor valor de exportaciones.',title_font_size=30, yaxis=dict(title='Valor exportacion',title_font_size=25), xaxis=dict(title='Pais',title_font_size=25))
fig.update_traces(overwrite=True, marker={"opacity": 0.5})
fig.write_html('R1_A1.html', auto_open=True)


##Reporte 2
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext("local", "first app")
spark = SparkSession(sc)
import plotly.graph_objects as go
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

text_file = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', quote='"', delimiter=',').load("C:\\Users\\sharolin\\Desktop\\entrada\\GuatemalaExportsTo.csv")
rddfiltro = text_file.rdd.map(tuple)
rddPAIS = rddfiltro.map(lambda word: (word[4],word[1]))
rddPAIS.take(5)


rddCONTEO=rddPAIS.reduceByKey(lambda a,b: a+b)
print("Conteo total -> %s" % rddCONTEO.collect())


rddORDEN = sc.parallelize(rddCONTEO.sortBy(lambda a: a[1],True).take(5))
print("Paises con menor valor de exportaciones -> %s" % rddORDEN.collect())


rddNombres = rddORDEN.map(lambda x: (x[0]))
rddTotales = rddORDEN.map(lambda x: (x[1]))
print(rddNombres.collect())
print(rddTotales.collect())


fig = go.Figure(data=go.Pie(labels=rddNombres.collect(),values=rddTotales.collect()))
fig.update_layout(title_text='Paises con menor valor de exportaciones',title_font_size=30)
fig.update_traces(hoverinfo='label+percent', textinfo='value',textfont_size=28)
fig.write_html('R2_A1.html', auto_open=True)



##Archivo 2
##Reporte 1
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext("local", "first app")
spark = SparkSession(sc)
import plotly.graph_objects as go
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

text_file = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', quote='"', delimiter=',').load("C:\\Users\\sharolin\\Desktop\\entrada\\TraficoAereoGt.csv")
rddfiltro = text_file.rdd.map(tuple)
rddAERO = rddfiltro.map(lambda word: (word[2],word[3]))
rddAERO.collect()


rddCONTEO=rddAERO.reduceByKey(lambda a,b: a+b)
print("Conteo total -> %s" % rddCONTEO.collect())


rddNombres = rddCONTEO.map(lambda x: (x[0]))
rddTotales = rddCONTEO.map(lambda x: (x[1]))
print(rddNombres.collect())
print(rddTotales.collect())

fig = go.Figure(data=go.Bar(x=rddNombres.collect(),y=rddTotales.collect()))
fig.update_layout(title_text='Aterrizajes por aeropuerto',title_font_size=30, yaxis=dict(title='Numero de Aterrizajes',title_font_size=25), xaxis=dict(title='Aerepuerto',title_font_size=25))
fig.update_traces(overwrite=True, marker={"opacity": 0.5})
fig.write_html('R1_A2.html', auto_open=True)


##Reporte 2

import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext("local", "first app")
spark = SparkSession(sc)
import plotly.graph_objects as go
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

text_file = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', quote='"', delimiter=',').load("C:\\Users\\sharolin\\Desktop\\entrada\\TraficoAereoGt.csv")
rddfiltro = text_file.rdd.map(tuple)
rddAERO = rddfiltro.map(lambda word: (word[0].split('/')[1],word[5]))
rddAERO.take(3)

rddCONTEO=rddAERO.reduceByKey(lambda a,b: a+b)
print("Conteo total -> %s" % rddCONTEO.collect())

rddORDEN=sc.parallelize(rddCONTEO.sortBy(lambda a: a[1],False).take(3))
print("meses con mayor numero de pasajeros de salida  -> %s" % rddORDEN.collect())

rddNombres = rddORDEN.map(lambda x: (x[0]))
rddTotales = rddORDEN.map(lambda x: (x[1]))
print(rddNombres.collect())
print(rddTotales.collect())

fig = go.Figure(data=go.Pie(labels=rddNombres.collect(),values=rddTotales.collect()))
fig.update_layout(title_text='Meses con mayor numero de pasajeros de salida',title_font_size=30)
fig.update_traces(hoverinfo='label+percent', textinfo='value',textfont_size=28)
fig.write_html('R2_A2.html', auto_open=True)



##Archivo3
#Reporte 1
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext("local", "first app")
spark = SparkSession(sc)
import plotly.graph_objects as go
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


text_file = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', quote='"', delimiter=',').load("C:\\Users\\sharolin\\Desktop\\entrada\\Covid19.csv")
rddfiltro = text_file.rdd.map(tuple)
rddAERO = rddfiltro.map(lambda word: (word[7],word[5]))
rddAERO.take(5)


rddCONTEO=rddAERO.reduceByKey(lambda a,b: a+b).filter(lambda x: x[0]=="Cuba" or x[0]=="France" or x[0]=="Canada" or x[0]=="Singapore" or x[0]=="South_Korea")
print("Conteo total -> %s" % rddCONTEO.collect())


rddNombres = rddCONTEO.map(lambda x: (x[0]))
rddTotales = rddCONTEO.map(lambda x: (x[1]))
print(rddNombres.collect())
print(rddTotales.collect())


fig = go.Figure(data=go.Bar(x=rddNombres.collect(),y=rddTotales.collect()))
fig.update_layout(title_text='Casos de Covid 19 por Pais',title_font_size=30, yaxis=dict(title='Numero de casos de covid 19',title_font_size=25), xaxis=dict(title='Pais',title_font_size=25))
fig.update_traces(overwrite=True, marker={"opacity": 0.5})
fig.write_html('R1_A3.html', auto_open=True)






#Reporte 2
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext("local", "first app")
spark = SparkSession(sc)
import plotly.graph_objects as go
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


text_file = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', quote='"', delimiter=',').load("C:\\Users\\sharolin\\Desktop\\entrada\\Covid19.csv")
rddfiltro = text_file.rdd.map(tuple)
rddAERO = rddfiltro.map(lambda word: (word[7],word[5]))
rddAERO.take(5)

rddCONTEO=rddAERO.reduceByKey(lambda a,b: a+b)
print("Conteo total -> %s" % rddCONTEO.collect())


rddORDEN = sc.parallelize(rddCONTEO.sortBy(lambda a: a[1],True).take(5))
print("Meses con menos casos de COVID  -> %s" % rddORDEN.collect())


rddNombres = rddORDEN.map(lambda x: (x[0]))
rddTotales = rddORDEN.map(lambda x: (x[1]))
print(rddNombres.collect())
print(rddTotales.collect())


fig = go.Figure(data=go.Pie(labels=rddNombres.collect(),values=rddTotales.collect()))
fig.update_layout(title_text='Meses con menos casos de COVID',title_font_size=30)
fig.update_traces(hoverinfo='label+percent', textinfo='value',textfont_size=28)
fig.write_html('R2_A3.html', auto_open=True)






#Reporte 3
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext("local", "first app")
spark = SparkSession(sc)
import plotly.graph_objects as go
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#pais, mes, casos, muertes
text_file = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', quote='"', delimiter=',').load("C:\\Users\\sharolin\\Desktop\\entrada\\Covid19.csv")
rddfiltro = text_file.rdd.map(tuple)
rddAERO = rddfiltro.map(lambda word: (word[7],word[3],word[5],word[6]))
rddAERO.take(5)


tablaGT = rddAERO.filter(lambda x: "Guatemala" in x[0] and "August" in x[1])
tablaCasos = tablaGT.map(lambda word: (word[0],word[2]))
tablaMuertes = tablaGT.map(lambda word: (word[0],word[3]))
tablaTotalCasos = tablaCasos.reduceByKey(lambda a,b: a+b)
tablaTotalMuertes = tablaMuertes.reduceByKey(lambda a,b: a+b)
print("Cosos totales -> %s" % tablaTotalCasos.collect())
print("Muertes totales -> %s" % tablaTotalMuertes.collect())

tablaunion = sc.union([tablaTotalCasos,tablaTotalMuertes])
tablaunion.take(2)

newRDD= sc.parallelize(["Casos", "Muertes"])
rddTotales = tablaunion.map(lambda x: (x[1]))
print(newRDD.collect())
print(rddTotales.collect())


fig = go.Figure(data=go.Pie(labels=newRDD.collect(),values=rddTotales.collect()))
fig.update_layout(title_text='Cantidad de Casos y Muertes en Guatemala por Covid 19 en el mes de Agosto',title_font_size=30)
fig.update_traces(hoverinfo='label+percent', textinfo='value',textfont_size=28)
fig.write_html('R3_A3.html', auto_open=True)