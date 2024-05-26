# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC **Título del proyecto:** Análisis de datos para la mejora de la seguridad pública en el estado de Nueva York
# MAGIC
# MAGIC **Integrantes:**
# MAGIC
# MAGIC * **Nombre:** Diego Alejandro Herrera Paz
# MAGIC * **Cargo:** Estudiante Ciencia de Datos
# MAGIC * **Correo electrónico:** diego-herrerap@javeriana.edu.co    
# MAGIC     
# MAGIC * **Nombre:** Sofía Catalina Galindo Mora
# MAGIC * **Cargo:** Estudiante Ciencia de Datos
# MAGIC * **Correo electrónico:** soofia.galindom@javeriana.edu.co   
# MAGIC     
# MAGIC * **Nombre:** Anamaria Leguizamón Alarcón
# MAGIC * **Cargo:** Estudiante Ciencia de Datos
# MAGIC * **Correo electrónico:** aleguizamona@javeriana.edu.co
# MAGIC
# MAGIC **Fecha:** 2024-04-10
# MAGIC
# MAGIC **Descripción del proyecto:**
# MAGIC
# MAGIC El estado de Nueva York enfrenta un desafío significativo en materia de seguridad pública, con altos índices de arrestos y accidentes viales. Para abordar este problema, el gobierno ha contratado a un equipo de consultores para desarrollar un plan de acción basado en el análisis de datos.
# MAGIC
# MAGIC El proyecto se enfocará en:
# MAGIC
# MAGIC * Análisis de los datos históricos de arrestos y accidentes viales para identificar patrones y tendencias.
# MAGIC * Identificación de los factores que contribuyen a estos problemas.
# MAGIC * Desarrollo de estrategias y soluciones basadas en datos para reducir la cantidad de arrestos y accidentes viales
# MAGIC
# MAGIC **Herramientas utilizadas:**
# MAGIC
# MAGIC * Databricks
# MAGIC * Python
# MAGIC * Spark
# MAGIC * Pandas
# MAGIC
# MAGIC **Notas adicionales:**
# MAGIC
# MAGIC Este proyecto se realizó como parte del curso de Procesamiento de Datos a Gran Escala de la Pontificia Universidad Javeriana.
# MAGIC
# MAGIC **Imagen:**
# MAGIC
# MAGIC ![Bogotá](https://posgrado.co/wp-content/uploads/2023/09/maestrias-universidad-javeriana.jpg)
# MAGIC

# COMMAND ----------

#Se iporta PySpark
import pyspark #contiene todas las funciones principales de PySpark
from pyspark import SparkContext #RDD
from pyspark.sql import SQLContext, Row #funcionalidad para trabajar datos estructurados y row para representar una fila.
from pyspark.sql.functions import col #col para representar una columna.
from pyspark.sql.types import IntegerType, FloatType #para especificar los datos de las columnas. 
import pandas as pd #biblioteca para el análisis de datos.

# COMMAND ----------

#se levanta la sesion PySpark, para hacer uso de los métodos y herramientas que dispone.
sc= SparkContext.getOrCreate() #se crea el contexto spark para interactuar con sus funciones.
sql_sc = SQLContext(sc) #contexto sql con el que se puede ejecutar consultas sql sobre los datos directamente.
sc #se imprime para verificar que se haya creado correctamente.

# COMMAND ----------

# MAGIC %md
# MAGIC #Datos de arrestos del Departamento de Policía de Nueva York hasta la fecha

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/NYPD_Arrest_Data__Year_to_Date__20240402-1.csv"))


# COMMAND ----------

df1 = spark.read.format("csv").option("inferSchema", True).option("header", True).option("step","," ).load("/FileStore/tables/NYPD_Arrest_Data__Year_to_Date__20240402-1.csv")
display (df1)
print (df1.count())

# COMMAND ----------

df1.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC #Colisiones de vehículos motorizados - Vehículos

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Motor_Vehicle_Collisions___Vehicles_20240402.csv"))

# COMMAND ----------

df3 = spark.read.format("csv").option("inferSchema", True).option("header", True).option("step","," ).load("/FileStore/tables/Motor_Vehicle_Collisions___Vehicles_20240402.csv")
display (df3)
print (df3.count())

# COMMAND ----------

df3.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC #4 Exploración de los datos

# COMMAND ----------

display(df1.describe())

# COMMAND ----------

display(df3.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #Datos de arrestos del Departamento de Policía de Nueva York hasta la fecha

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import pyspark.sql.functions as F

# COMMAND ----------

# Conteo de valores únicos para variables categóricas
df1.select("PERP_SEX", "PERP_RACE", "PD_DESC").groupBy("PERP_SEX", "PERP_RACE", "PD_DESC").count().show()


# COMMAND ----------

# Obtener los datos para ARREST_KEY
arrest_key_data = df1.select('ARREST_KEY').rdd.flatMap(lambda x: x).collect()

# Crear histograma
plt.figure(figsize=(8, 6))
plt.hist(arrest_key_data, bins=30, color='skyblue', edgecolor='black')
plt.title('Histograma de ARREST_KEY')
plt.xlabel('ARREST_KEY')
plt.ylabel('Frecuencia')
plt.show()


# COMMAND ----------



# Filtrar los valores nulos en la columna 'PD_CD'
pd_cd_data = df1.select('PD_CD').filter(df1['PD_CD'].isNotNull()).rdd.flatMap(lambda x: x).collect()

# Crear histograma
plt.figure(figsize=(8, 6))
plt.hist(pd_cd_data, bins=30, color='skyblue', edgecolor='black')
plt.title('Histograma de PD_CD')
plt.xlabel('PD_CD')
plt.ylabel('Frecuencia')
plt.show()


# COMMAND ----------



# Filtrar los valores nulos en la columna 'KY_CD'
ky_cd_data = df1.select('KY_CD').filter(df1['KY_CD'].isNotNull()).rdd.flatMap(lambda x: x).collect()

# Crear histograma
plt.figure(figsize=(8, 6))
plt.hist(ky_cd_data, bins=30, color='skyblue', edgecolor='black')
plt.title('Histograma de KY_CD')
plt.xlabel('KY_CD')
plt.ylabel('Frecuencia')
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt

# Obtener los datos para X_COORD_CD
x_coord_data = df1.select('X_COORD_CD').rdd.flatMap(lambda x: x).collect()

# Crear histograma para X_COORD_CD
plt.figure(figsize=(8, 6))
plt.hist(x_coord_data, bins=30, color='skyblue', edgecolor='black')
plt.title('Histograma de X_COORD_CD')
plt.xlabel('X_COORD_CD')
plt.ylabel('Frecuencia')
plt.show()

# Obtener los datos para Y_COORD_CD
y_coord_data = df1.select('Y_COORD_CD').rdd.flatMap(lambda x: x).collect()

# Crear histograma para Y_COORD_CD
plt.figure(figsize=(8, 6))
plt.hist(y_coord_data, bins=30, color='skyblue', edgecolor='black')
plt.title('Histograma de Y_COORD_CD')
plt.xlabel('Y_COORD_CD')
plt.ylabel('Frecuencia')
plt.show()


# COMMAND ----------

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

# Seleccionar solo las variables numéricas
variables_numericas = df1.select("ARREST_PRECINCT", "ARREST_KEY", "X_COORD_CD", "Y_COORD_CD", "Latitude", "Longitude")

# Convertir las variables a un solo vector
ensamblador = VectorAssembler(inputCols=variables_numericas.columns, outputCol="features")
variables_ensambladas = ensamblador.transform(variables_numericas)

# Calcular la matriz de correlación
matriz_correlacion = Correlation.corr(variables_ensambladas, "features").head()

# Extraer la matriz de correlación
matriz_correlacion = matriz_correlacion[0].toArray()


# COMMAND ----------

import seaborn as sns

# Convertir la matriz de correlación en un DataFrame de pandas para usar con seaborn
matriz_correlacion_df = pd.DataFrame(matriz_correlacion, columns=variables_numericas.columns, index=variables_numericas.columns)

# Crear un mapa de calor de la matriz de correlación
plt.figure(figsize=(10, 8))
sns.heatmap(matriz_correlacion_df, annot=True, cmap='coolwarm', linewidths=0.5)
plt.title('Matriz de Correlación')
plt.show()


# COMMAND ----------

# Obtener los recuentos de las diferentes categorías de PERP_SEX
perp_sex_counts = df1.groupBy('PERP_SEX').count().orderBy('count', ascending=False)

# Obtener los datos para graficar
perp_sex = perp_sex_counts.select('PERP_SEX').rdd.map(lambda row: row[0]).collect()
counts = perp_sex_counts.select('count').rdd.map(lambda row: row[0]).collect()

# Graficar
plt.figure(figsize=(6, 4))
plt.bar(perp_sex, counts, color='skyblue')
plt.xlabel('Sexo del Detenido')
plt.ylabel('Cantidad de Arrestos')
plt.title('Distribución de Arrestos por Sexo')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

from pyspark.sql.functions import col

# Filtrar los datos para excluir registros con valores nulos en 'LAW_CAT_CD'
df_filtered = df1.filter(col('LAW_CAT_CD').isNotNull())

# Obtener los recuentos de las diferentes categorías de LAW_CAT_CD
law_cat_counts = df_filtered.groupBy('LAW_CAT_CD').count().orderBy('count', ascending=False)

# Obtener los datos para graficar
law_cat = law_cat_counts.select('LAW_CAT_CD').rdd.map(lambda row: row[0]).collect()
counts = law_cat_counts.select('count').rdd.map(lambda row: row[0]).collect()

# Graficar
plt.figure(figsize=(6, 4))
plt.bar(law_cat, counts, color='skyblue')
plt.xlabel('Categoría Legal del Delito')
plt.ylabel('Cantidad de Arrestos')
plt.title('Distribución de Arrestos por Categoría Legal del Delito')
plt.xticks(rotation=45)
plt.show()



# COMMAND ----------

# Obtener los recuentos de las diferentes categorías de ARREST_BORO
arrest_boro_counts = df1.groupBy('ARREST_BORO').count().orderBy('count', ascending=False)

# Obtener los datos para graficar
arrest_boro = arrest_boro_counts.select('ARREST_BORO').rdd.map(lambda row: row[0]).collect()
counts = arrest_boro_counts.select('count').rdd.map(lambda row: row[0]).collect()

# Graficar
plt.figure(figsize=(10, 6))
plt.bar(arrest_boro, counts, color='skyblue')
plt.xlabel('Distrito de Arresto')
plt.ylabel('Cantidad de Arrestos')
plt.title('Distribución de Arrestos por Distrito')
plt.xticks(rotation=45)
plt.show()


# COMMAND ----------


# Histograma de edades
df1.select("AGE_GROUP").toPandas()["AGE_GROUP"].value_counts().plot(kind='bar')
plt.title('Distribución de Edades')
plt.xlabel('Grupo de Edad')
plt.ylabel('Frecuencia')
plt.show()


# COMMAND ----------


# Contar la cantidad de arrestos por descripción de delito
delito_count = df1.groupBy("PD_DESC").count().orderBy(F.desc("count"))

# Convertir los datos a pandas DataFrame para visualización con Matplotlib
delito_count_pd = delito_count.toPandas()

#Graficar los delitos más cometidos
plt.figure(figsize=(10, 6))
plt.barh(delito_count_pd["PD_DESC"].head(10), delito_count_pd["count"].head(10), color='skyblue')
plt.xlabel('Cantidad de Arrestos')
plt.ylabel('Descripción de Delito')
plt.title('Top 10 Delitos Más Cometidos en New York')
plt.gca().invert_yaxis()  # Invertir el eje y para mostrar el delito más común en la parte superior
plt.show()



# COMMAND ----------

# Extraer el año de la columna ARREST_DATE
df1 = df1.withColumn("ARREST_YEAR", F.year("ARREST_DATE"))

# Obtener los años únicos presentes en el conjunto de datos
years = df1.select("ARREST_YEAR").distinct().orderBy("ARREST_YEAR").rdd.flatMap(lambda x: x).collect()

# Mostrar los años
print("Años presentes en el conjunto de datos:")
for year in years:
    print(year)

# COMMAND ----------

# Extraer el mes de la columna ARREST_DATE
df1 = df1.withColumn("ARREST_MONTH", F.month("ARREST_DATE"))

# Contar la cantidad de arrestos por mes
delitos_por_mes = df1.groupBy("ARREST_MONTH").count().orderBy("ARREST_MONTH")

# Convertir los datos a pandas DataFrame para visualización con Matplotlib
delitos_por_mes_pd = delitos_por_mes.toPandas()

# Graficar la cantidad de arrestos por mes
meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
plt.figure(figsize=(10, 6))
plt.bar(delitos_por_mes_pd["ARREST_MONTH"], delitos_por_mes_pd["count"], color='skyblue')
plt.xlabel('Mes')
plt.ylabel('Cantidad de Arrestos')
plt.title('Cantidad de Arrestos por Mes en New York')
plt.xticks(delitos_por_mes_pd["ARREST_MONTH"], meses, rotation=45)
plt.grid(True)
plt.show()

# COMMAND ----------

# Contar la cantidad de arrestos por tipo de detención y municipio
detenciones_por_municipio = df1.groupBy("ARREST_BORO", "LAW_CAT_CD").count()

# Encontrar el tipo de detención más común por municipio
detenciones_maximas_por_municipio = detenciones_por_municipio.groupBy("ARREST_BORO").agg(
    F.max(F.struct(F.col("count"), F.col("LAW_CAT_CD"))).alias("max_detencion")
)

# Unir con la tabla original para obtener el tipo de detención
resultado = detenciones_maximas_por_municipio.alias("dm").join(
    detenciones_por_municipio.alias("dp"),
    (F.col("dm.ARREST_BORO") == F.col("dp.ARREST_BORO")) & 
    (F.col("dm.max_detencion")["count"] == F.col("dp.count")) &
    (F.col("dm.max_detencion")["LAW_CAT_CD"] == F.col("dp.LAW_CAT_CD")),
    "inner"
).select(
    F.col("dm.ARREST_BORO"),
    F.col("dp.LAW_CAT_CD"),
    F.col("dp.count").alias("Cantidad")
)

# Mostrar el resultado
# felony, misdemeanor, violation
# Borough of arrest. B(Bronx), S(Staten Island), K(Brooklyn), M(Manhattan), Q(Queens)
resultado.show()

# COMMAND ----------

# Contar la cantidad de delitos por raza
delitos_por_raza = df1.groupBy("PERP_RACE").count().orderBy(F.desc("count"))

# Convertir los datos a pandas DataFrame para visualización con Matplotlib
delitos_por_raza_pd = delitos_por_raza.toPandas()

# Graficar la cantidad de delitos por raza
plt.figure(figsize=(10, 6))
plt.bar(delitos_por_raza_pd["PERP_RACE"], delitos_por_raza_pd["count"], color='skyblue')
plt.xlabel('Raza')
plt.ylabel('Cantidad de Delitos')
plt.title('Cantidad de Delitos Cometidos por Raza')
plt.xticks(rotation=45)
plt.grid(True)
plt.show()

# COMMAND ----------

# Proporción de delitos por categoría
plt.figure(figsize=(8, 8))
df1.groupBy("LAW_CAT_CD").count().toPandas().plot(kind='pie', y='count', autopct='%1.1f%%', labels=None)
plt.title('Proporción de Delitos por Categoría')
plt.ylabel('')
plt.legend(labels=df1.select("LAW_CAT_CD").distinct().rdd.map(lambda row: row[0]).collect(), loc='upper right')
plt.show()



# COMMAND ----------

# Imprimir los valores únicos de la columna "LAW_CAT_CD"
valores_law_cat_cd = df1.select("LAW_CAT_CD").distinct().rdd.map(lambda row: row[0]).collect()
print("Valores únicos de LAW_CAT_CD:", valores_law_cat_cd)


# COMMAND ----------

# MAGIC %md
# MAGIC #Colisiones de vehículos motorizados - Vehículos

# COMMAND ----------

from pyspark.sql.functions import hour

# 1. Relación entre momentos del día de los accidentes y la frecuencia
# Crear una columna para la hora del día a partir de la columna CRASH_TIME
df3 = df3.withColumn('CRASH_HOUR', hour(df3['CRASH_TIME']))

# Histograma de la distribución de los momentos del día de los accidentes (CRASH_HOUR)
plt.figure(figsize=(10, 6))
sns.histplot(data=df3.toPandas(), x='CRASH_HOUR', bins=24, kde=True)
plt.title('Distribución de accidentes por momento del día')
plt.xlabel('Hora del día')
plt.ylabel('Frecuencia de accidentes')
plt.xticks(range(0, 24))
plt.show()



# COMMAND ----------

from pyspark.sql.functions import count

# Contar la frecuencia de accidentes para cada sexo
accidents_by_sex = df3.groupBy('DRIVER_SEX').agg(count('UNIQUE_ID').alias('accidents_count'))

# Convertir el resultado a un DataFrame de pandas para visualización
accidents_by_sex_pd = accidents_by_sex.toPandas()

# Graficar la relación entre el sexo y el número de accidentes
plt.figure(figsize=(8, 6))
sns.barplot(data=accidents_by_sex_pd, x='DRIVER_SEX', y='accidents_count', color='skyblue')
plt.title('Relación entre el sexo y el número de accidentes')
plt.xlabel('Sexo del conductor')
plt.ylabel('Número de accidentes')
plt.show()


# COMMAND ----------

from pyspark.sql.functions import desc

# Contar la frecuencia de cada tipo de precrash
precrash_counts = df3.groupBy('PRE_CRASH').count()

# Encontrar el tipo de precrash más común
most_common_precrash = precrash_counts.orderBy(desc('count')).first()

print("El tipo de precrash más común es:", most_common_precrash['PRE_CRASH'])


# COMMAND ----------

# Obtener los recuentos de accidentes por tipo de vehículo
vehicle_type_counts = df3.groupBy('VEHICLE_TYPE').count().orderBy('count', ascending=False)

# Imprimir los recuentos de accidentes por tipo de vehículo
vehicle_type_counts.show()



# COMMAND ----------

import matplotlib.pyplot as plt

# Filtrar valores nulos en CRASH_DATE
df3_filtered = df3.filter(df3['CRASH_DATE'].isNotNull())

# Obtener los datos para CRASH_DATE
crash_dates = df3_filtered.select('CRASH_DATE').rdd.flatMap(lambda x: x).collect()

# Crear histograma para CRASH_DATE
plt.figure(figsize=(10, 6))
plt.hist(crash_dates, bins=30, color='skyblue', edgecolor='black')
plt.title('Histograma de Fecha de Accidente')
plt.xlabel('Fecha de Accidente')
plt.ylabel('Frecuencia')
plt.xticks(rotation=45)
plt.show()




# COMMAND ----------

import matplotlib.pyplot as plt

# Filtrar valores nulos en VEHICLE_DAMAGE
df3_filtered = df3.filter(df3['VEHICLE_DAMAGE'].isNotNull())

# Obtener los datos para VEHICLE_DAMAGE
vehicle_damage_data = df3_filtered.select('VEHICLE_DAMAGE').rdd.flatMap(lambda x: x).collect()

# Crear histograma para VEHICLE_DAMAGE
plt.figure(figsize=(8, 6))
plt.hist(vehicle_damage_data, bins=30, color='skyblue', edgecolor='black')
plt.title('Histograma de Daños a Vehículos')
plt.xlabel('Tipo de Daño')
plt.ylabel('Frecuencia')
plt.xticks(rotation=45)
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt

# Filtrar valores nulos en VEHICLE_TYPE
df3_filtered = df3.filter(df3['VEHICLE_TYPE'].isNotNull())

# Obtener los recuentos de los tipos de vehículos
vehicle_type_counts = df3_filtered.groupBy('VEHICLE_TYPE').count().orderBy('count', ascending=False).limit(10)

# Obtener los datos para graficar
vehicle_types = vehicle_type_counts.select('VEHICLE_TYPE').rdd.map(lambda row: row[0]).collect()
counts = vehicle_type_counts.select('count').rdd.map(lambda row: row[0]).collect()

# Graficar
plt.figure(figsize=(10, 6))
plt.bar(vehicle_types, counts, color='skyblue')
plt.xlabel('Tipo de Vehículo')
plt.ylabel('Cantidad de Accidentes')
plt.title('Top 10 de Tipos de Vehículos Involucrados en Accidentes')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()


# COMMAND ----------

# Obtener los valores únicos en DRIVER_LICENSE_STATUS
unique_license_status = df3.select('DRIVER_LICENSE_STATUS').distinct().rdd.map(lambda row: row[0]).collect()

# Imprimir los valores únicos
print(unique_license_status)


# COMMAND ----------

import matplotlib.pyplot as plt

# Filtrar valores nulos en DRIVER_LICENSE_STATUS y DRIVER_SEX
df3_filtered = df3.filter(df3['DRIVER_LICENSE_STATUS'].isNotNull() & df3['DRIVER_SEX'].isNotNull())

# Obtener los recuentos de vehículos involucrados en colisiones según el estado de la licencia de conducir y el sexo del conductor
collision_counts = df3_filtered.groupby(['DRIVER_LICENSE_STATUS', 'DRIVER_SEX']).count()

# Convertir los resultados en un formato adecuado para graficar
data = collision_counts.groupby('DRIVER_LICENSE_STATUS').pivot('DRIVER_SEX').sum('count').fillna(0).collect()

# Preparar los datos para graficar
license_status = [row['DRIVER_LICENSE_STATUS'] for row in data]
male_counts = [row['M'] for row in data]
female_counts = [row['F'] for row in data]

# Graficar
x = range(len(license_status))
width = 0.35

fig, ax = plt.subplots(figsize=(10, 6))
bars1 = ax.bar(x, male_counts, width, label='Masculino')
bars2 = ax.bar([p + width for p in x], female_counts, width, label='Femenino')

ax.set_xlabel('Estado de la Licencia de Conducir')
ax.set_ylabel('Cantidad de Vehículos')
ax.set_title('Cantidad de Vehículos Involucrados en Colisiones por Estado de la Licencia de Conducir y Sexo del Conductor')
ax.set_xticks([p + width / 2 for p in x])
ax.set_xticklabels(license_status)
ax.legend()

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Filtros, limpieza y transformación inicial: 

# COMMAND ----------

# MAGIC %md
# MAGIC **Duplicados**

# COMMAND ----------

from pyspark.sql.functions import col

# Eliminar registros duplicados basados en las columnas especificadas
df1_sin_duplicados = df1.dropDuplicates(['ARREST_KEY', 'ARREST_DATE', 'PD_CD', 'PD_DESC', 'KY_CD', 'OFNS_DESC', 'LAW_CODE', 'LAW_CAT_CD'])

# Mostrar los registros duplicados si existen
registros_duplicados = df1.subtract(df1_sin_duplicados)
if registros_duplicados.count() > 0:
    print("Registros duplicados:")
    registros_duplicados.show(truncate=False)
else:
    print("No hay datos duplicados.")

# COMMAND ----------

from pyspark.sql.functions import col

# Eliminar registros duplicados basados en las columnas especificadas
df3_sin_duplicados = df3.dropDuplicates()

# Mostrar los registros duplicados si existen
registros_duplicados = df3.subtract(df3_sin_duplicados)
if registros_duplicados.count() > 0:
    print("Registros duplicados:")
    registros_duplicados.show(truncate=False)
else:
    print("No hay datos duplicados.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Nulos**

# COMMAND ----------

from pyspark.sql.functions import count, when, isnan

# Luego puedes usar count y otras funciones sin problemas
df1.select([count(when(col(c).isNull(), c)).alias(c) for c in df1.columns]).show()

# COMMAND ----------

# Imputar valores 'Desconocido' en la columna PD_CD y KY_CD
df1 = df1.fillna({'PD_CD': 'Desconocido'})
df1 = df1.fillna({'KY_CD': 'Desconocido'})


# COMMAND ----------

df3.select([count(when(col(c).isNull(), c)).alias(c) for c in df3.columns]).show()

# COMMAND ----------

from pyspark.sql.functions import when
df3 = df3.withColumn("VEHICLE_OCCUPANTS", df3["VEHICLE_OCCUPANTS"].cast("string"))
df3 = df3.withColumn("Ocupantes_Categoria", when(df3["VEHICLE_OCCUPANTS"] == "1", "1 Ocupante")
                    .when(df3["VEHICLE_OCCUPANTS"] == "2", "2 Ocupantes")
                    .when(df3["VEHICLE_OCCUPANTS"] == "3", "3 Ocupantes")
                    .when(df3["VEHICLE_OCCUPANTS"] == "4", "4 Ocupantes")
                    .when(df3["VEHICLE_OCCUPANTS"] == "5", "5 Ocupantes")
                    .otherwise("Más de 5 Ocupantes"))
df3 = df3.fillna({"VEHICLE_OCCUPANTS": "0"})
df3 = df3.withColumn("Ocupantes_Categoria", when(df3["VEHICLE_OCCUPANTS"] == "0", "Desconocido")
                    .otherwise(df3["Ocupantes_Categoria"]))
df3.groupby("Ocupantes_Categoria").count().show()


# COMMAND ----------

# Imputar valores 'Desconocido' en la columna PD_CD y KY_CD
df3 = df3.fillna({'STATE_REGISTRATION': 'Desconocido'})
df3 = df3.fillna({'VEHICLE_MAKE': 'Desconocido'})
df3 = df3.fillna({'VEHICLE_MODEL': 'Desconocido'})
df3 = df3.fillna({'DRIVER_SEX': 'N'})
df3 = df3.fillna({'DRIVER_LICENSE_JURISDICTION': 'Desconocido'})


# COMMAND ----------

from pyspark.sql.functions import col, mode

moda_damagev = df3.select(mode(col("VEHICLE_DAMAGE"))).rdd.flatMap(lambda x: x).collect()[0]
df3 = df3.fillna({"VEHICLE_DAMAGE": moda_damagev})


moda_damagep = df3.select(mode(col("PUBLIC_PROPERTY_DAMAGE"))).rdd.flatMap(lambda x: x).collect()[0]
df3 = df3.fillna({"PUBLIC_PROPERTY_DAMAGE": moda_damagep})


moda_damagept = df3.select(mode(col("PUBLIC_PROPERTY_DAMAGE_TYPE"))).rdd.flatMap(lambda x: x).collect()[0]
df3 = df3.fillna({"PUBLIC_PROPERTY_DAMAGE_TYPE": moda_damagept})

# COMMAND ----------

df3.select([count(when(col(c).isNull(), c)).alias(c) for c in df3.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Entrega dos

# COMMAND ----------

# MAGIC %md
# MAGIC **Filtros y limpieza:**

# COMMAND ----------

# MAGIC %md
# MAGIC Nos dimos cuenta que hay variables que no necesitamos para responder nuestras preguntas, así que empezamos eliminando esas columnas.

# COMMAND ----------

columns_to_drop_arrest = [
    "ARREST_KEY",
    "PD_CD",
    "KY_CD",
    "JURISDICTION_CODE",
    "X_COORD_CD",  # Si se usa Latitude y Longitude
    "Y_COORD_CD",  # Si se usa Latitude y Longitude
    "New Georeferenced Column"
]

df1_cleaned = df1.drop(*columns_to_drop_arrest)

# Mostrar las primeras filas del DataFrame limpio
df1_cleaned.show()

# COMMAND ----------

columns_to_drop_vehicle = [
    "UNIQUE_ID",
    "COLLISION_ID",
    "STATE_REGISTRATION",
    "VEHICLE_MAKE",
    "VEHICLE_YEAR",
    "DRIVER_SEX",
    "DRIVER_LICENSE_STATUS",
    "DRIVER_LICENSE_JURISDICTION",
    "POINT_OF_IMPACT",
    "VEHICLE_OCCUPANTS",
    "PUBLIC_PROPERTY_DAMAGE",
    "PUBLIC_PROPERTY_DAMAGE_TYPE",
    "CONTRIBUTING_FACTOR_1",
    "CONTRIBUTING_FACTOR_2",
    "VEHICLE_DAMAGE_1",
    "VEHICLE_DAMAGE_2",
    "VEHICLE_DAMAGE_3", 
    "TRAVEL_DIRECTION"
]

df3_cleaned = df3.drop(*columns_to_drop_vehicle)

# Mostrar las primeras filas del DataFrame limpio
df3_cleaned.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Contamos nulos:

# COMMAND ----------

from pyspark.sql.functions import count, when, isnan

df1_cleaned.select([count(when(col(c).isNull(), c)).alias(c) for c in df1_cleaned.columns]).show()

# COMMAND ----------

df3_cleaned.select([count(when(col(c).isNull(), c)).alias(c) for c in df3_cleaned.columns]).show()

# COMMAND ----------

from pyspark.sql.functions import col, when, mean, lit, concat_ws, year, month
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# COMMAND ----------

mode_window = Window.partitionBy("AGE_GROUP", "PERP_SEX")
mode_df = (df1_cleaned
           .groupBy("AGE_GROUP", "PERP_SEX", "LAW_CAT_CD")
           .agg(count("LAW_CAT_CD").alias("count_law_cat_cd"))
           .withColumn("rank", F.row_number().over(mode_window.orderBy(F.desc("count_law_cat_cd"))))
           .filter(col("rank") == 1)
           .select("AGE_GROUP", "PERP_SEX", col("LAW_CAT_CD").alias("mode_law_cat_cd")))

# COMMAND ----------

df1_cleaned = (df1_cleaned
              .join(mode_df, on=["AGE_GROUP", "PERP_SEX"], how="left")
              .withColumn("LAW_CAT_CD",
                          F.when(col("LAW_CAT_CD").isNull(), col("mode_law_cat_cd"))
                          .otherwise(col("LAW_CAT_CD")))
              .drop("mode_law_cat_cd"))


# COMMAND ----------

df1_cleaned.select([count(when(col(c).isNull(), c)).alias(c) for c in df1_cleaned.columns]).show()

# COMMAND ----------

# 3. Realizar transformaciones
# Convertir 'ARREST_DATE' a formato datetime y extraer el año y mes
df1_cleaned = df1_cleaned.withColumn('ARREST_DATE', col('ARREST_DATE').cast('timestamp'))
df1_cleaned = df1_cleaned.withColumn('ARREST_YEAR', year(col('ARREST_DATE')))
df1_cleaned = df1_cleaned.withColumn('ARREST_MONTH', month(col('ARREST_DATE')))


# Convertir coordenadas a un solo campo 'LOCATION' como tupla
df1_cleaned = df1_cleaned.withColumn('LOCATION', concat_ws(', ', col('Latitude'), col('Longitude')))

# Mostrar el dataframe procesado
df1_cleaned.show()


# COMMAND ----------

from pyspark.sql.functions import year, month

# Filtrar valores nulos en CRASH_DATE
df3_cleaned = df3_cleaned.filter(df3_cleaned['CRASH_DATE'].isNotNull())

# Extraer el año y el mes de CRASH_DATE
df3_cleaned = df3_cleaned.withColumn('CRASH_YEAR', year('CRASH_DATE'))
df3_cleaned = df3_cleaned.withColumn('CRASH_MONTH', month('CRASH_DATE'))

# Mostrar las primeras filas para verificar
df3_cleaned.select('CRASH_DATE', 'CRASH_YEAR', 'CRASH_MONTH').show()


# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, count, when, row_number, desc

# Definir ventana de partición por VEHICLE_MODEL
mode_window_vehicle = Window.partitionBy("VEHICLE_MODEL")
mode_window_damage = Window.partitionBy("VEHICLE_DAMAGE")

# Imputación de VEHICLE_TYPE
# Calcular la moda de VEHICLE_TYPE por VEHICLE_MODEL
mode_df_vehicle_type = (df3_cleaned
                        .groupBy("VEHICLE_MODEL", "VEHICLE_TYPE")
                        .agg(count("VEHICLE_TYPE").alias("count_vehicle_type"))
                        .withColumn("rank", row_number().over(mode_window_vehicle.orderBy(desc("count_vehicle_type"))))
                        .filter(col("rank") == 1)
                        .select("VEHICLE_MODEL", col("VEHICLE_TYPE").alias("mode_vehicle_type")))

# Unir el dataframe original con la moda y reemplazar valores nulos
df3_cleaned = (df3_cleaned
               .join(mode_df_vehicle_type, on=["VEHICLE_MODEL"], how="left")
               .withColumn("VEHICLE_TYPE", when(col("VEHICLE_TYPE").isNull(), col("mode_vehicle_type")).otherwise(col("VEHICLE_TYPE")))
               .drop("mode_vehicle_type"))

# Imputación de PRE_CRASH
# Calcular la moda de PRE_CRASH por VEHICLE_DAMAGE
mode_df_pre_crash = (df3_cleaned
                     .groupBy("VEHICLE_DAMAGE", "PRE_CRASH")
                     .agg(count("PRE_CRASH").alias("count_pre_crash"))
                     .withColumn("rank", row_number().over(mode_window_damage.orderBy(desc("count_pre_crash"))))
                     .filter(col("rank") == 1)
                     .select("VEHICLE_DAMAGE", col("PRE_CRASH").alias("mode_pre_crash")))

# Unir el dataframe original con la moda y reemplazar valores nulos
df3_cleaned = (df3_cleaned
               .join(mode_df_pre_crash, on=["VEHICLE_DAMAGE"], how="left")
               .withColumn("PRE_CRASH", when(col("PRE_CRASH").isNull(), col("mode_pre_crash")).otherwise(col("PRE_CRASH")))
               .drop("mode_pre_crash"))

# Mostrar el dataframe procesado
df3_cleaned.show()


# COMMAND ----------

df3_cleaned.select([count(when(col(c).isNull(), c)).alias(c) for c in df3_cleaned.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Solución preguntas:

# COMMAND ----------

# MAGIC %md
# MAGIC 6. ¿Qué tipos de delitos (PD_DESC y OFNS_DESC) son los más frecuentes y cómo se distribuyen geográficamente (ARREST_BORO, ARREST_PRECINCT, X_COORD_CD, Y_COORD_CD, Latitude, Longitude)?

# COMMAND ----------

# Contar la cantidad de arrestos por descripción de delito
delito_count = df1_cleaned.groupBy("PD_DESC").count().orderBy(F.desc("count"))

# Convertir los datos a pandas DataFrame para visualización con Matplotlib
delito_count_pd = delito_count.toPandas()

#Graficar los delitos más cometidos
plt.figure(figsize=(10, 6))
plt.barh(delito_count_pd["PD_DESC"].head(10), delito_count_pd["count"].head(10), color='skyblue')
plt.xlabel('Cantidad de Arrestos')
plt.ylabel('Descripción de Delito')
plt.title('Top 10 Delitos Más Cometidos en New York')
plt.gca().invert_yaxis()  # Invertir el eje y para mostrar el delito más común en la parte superior
plt.show()



# COMMAND ----------

from pyspark.sql.functions import col

# Filtrar datos donde la latitud y longitud son no nulas
filtered_data = df1_cleaned.filter(col("Latitude").isNotNull() & col("Longitude").isNotNull())

# Seleccionar solo las columnas necesarias
selected_data = filtered_data.select(col("Latitude"), col("Longitude"), col("OFNS_DESC"))

# COMMAND ----------

#Convertir a Pandas 
pandas_df = selected_data.toPandas()

# COMMAND ----------

pip install folium


# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/my_maps

# COMMAND ----------

import folium
from folium.plugins import MarkerCluster

# Crear el mapa
map = folium.Map(location=[35.7128, -74.0060],zoom_start=12)

# Crear un cluster de marcadores
marker_cluster = MarkerCluster().add_to(map)

# Añadir marcadores al cluster
for index, row in pandas_df.iterrows():
    folium.Marker(
        location=[row['Latitude'], row['Longitude']],
        popup=row['OFNS_DESC']
    ).add_to(marker_cluster)

# COMMAND ----------

# Convertir el mapa a HTML string
html_string = map.get_root().render()

# Guardar el string HTML como archivo
dbutils.fs.put("/FileStore/my_maps/nyc_arrests_map.html", html_string, True)

# COMMAND ----------

# MAGIC %md
# MAGIC Para visualizar el mapa, pegue este link en el navegador que esté manejando:
# MAGIC https://community.cloud.databricks.com/files/my_maps/nyc_arrests_map.html

# COMMAND ----------

# MAGIC %md
# MAGIC 7. ¿Existe alguna relación entre el nivel de delito (LAW_CAT_CD) y las características demográficas de los sospechosos, como su grupo de edad (AGE_GROUP) o género (PERP_SEX)?

# COMMAND ----------

# Obtener los recuentos de las diferentes categorías de PERP_SEX
perp_sex_counts = df1_cleaned.groupBy('PERP_SEX').count().orderBy('count', ascending=False)

# Obtener los datos para graficar
perp_sex = perp_sex_counts.select('PERP_SEX').rdd.map(lambda row: row[0]).collect()
counts = perp_sex_counts.select('count').rdd.map(lambda row: row[0]).collect()
# Graficar
plt.figure(figsize=(6, 4))
plt.bar(perp_sex, counts, color='skyblue')
plt.xlabel('Sexo del Detenido')
plt.ylabel('Cantidad de Arrestos')
plt.title('Distribución de Arrestos por Sexo')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------


# Histograma de edades
df1_cleaned.select("AGE_GROUP").toPandas()["AGE_GROUP"].value_counts().plot(kind='bar')
plt.title('Distribución de Edades')
plt.xlabel('Grupo de Edad')
plt.ylabel('Frecuencia')
plt.show()

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import DenseMatrix
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Convertir variables categóricas a numéricas
indexers = [
    StringIndexer(inputCol="LAW_CAT_CD", outputCol="LAW_CAT_CD_index"),
    StringIndexer(inputCol="AGE_GROUP", outputCol="AGE_GROUP_index"),
    StringIndexer(inputCol="PERP_SEX", outputCol="PERP_SEX_index")
]

# Pipeline para ejecutar las transformaciones
pipeline = Pipeline(stages=indexers)
df_indexed = pipeline.fit(df1_cleaned).transform(df1_cleaned)

# Vector Assembler para crear la matriz de características
assembler = VectorAssembler(
    inputCols=["LAW_CAT_CD_index", "AGE_GROUP_index", "PERP_SEX_index"],
    outputCol="features"
)

df_vector = assembler.transform(df_indexed).select("features")

# Calcular la matriz de correlación
correlation_matrix = Correlation.corr(df_vector, "features").head()[0]
correlation_array = correlation_matrix.toArray()

# Crear un DataFrame de Pandas para la matriz de correlación
columns = ["LAW_CAT_CD_index", "AGE_GROUP_index", "PERP_SEX_index"]
corr_df = pd.DataFrame(correlation_array, index=columns, columns=columns)

# Visualizar la matriz de correlación con Seaborn
plt.figure(figsize=(8, 6))
sns.heatmap(corr_df, annot=True, cmap="coolwarm", fmt=".2f")
plt.title('Matriz de Correlación entre Nivel de Delito y Características Demográficas')
plt.show()


# COMMAND ----------

# MAGIC  %md 8. ¿Cómo han variado las tasas de arrestos a lo largo del tiempo (ARREST DATE) y si
# MAGIC existen patrones temporales o estacionales en los diferentes tipos de delitos?

# COMMAND ----------

# Extraer el mes de la columna ARREST_DATE
df1_cleaned = df1_cleaned.withColumn("ARREST_MONTH", F.month("ARREST_DATE"))

# Contar la cantidad de arrestos por mes
delitos_por_mes = df1_cleaned.groupBy("ARREST_MONTH").count().orderBy("ARREST_MONTH")

# Convertir los datos a pandas DataFrame para visualización con Matplotlib
delitos_por_mes_pd = delitos_por_mes.toPandas()

# Graficar la cantidad de arrestos por mes
meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
plt.figure(figsize=(10, 6))
plt.bar(delitos_por_mes_pd["ARREST_MONTH"], delitos_por_mes_pd["count"], color='skyblue')
plt.xlabel('Mes')
plt.ylabel('Cantidad de Arrestos')
plt.title('Cantidad de Arrestos por Mes en New York')
plt.xticks(delitos_por_mes_pd["ARREST_MONTH"], meses, rotation=45)
plt.grid(True)
plt.show()

# COMMAND ----------

from pyspark.sql.functions import year, month, col

# Crear columnas de año y mes si no están ya creadas
df_time = df1_cleaned.withColumn("ARREST_YEAR", year(col("ARREST_DATE"))).withColumn("ARREST_MONTH", month(col("ARREST_DATE")))

# Agrupar por año y mes y contar los arrestos
arrest_trends = df_time.groupBy("ARREST_YEAR", "ARREST_MONTH").count().orderBy("ARREST_YEAR", "ARREST_MONTH")

# Mostrar los datos en una tabla
arrest_trends.show()

# Para visualizar las tendencias temporales
import matplotlib.pyplot as plt

# Recoger los datos
arrest_trends_pd = arrest_trends.toPandas()

# Graficar las tasas de arrestos
plt.figure(figsize=(12, 6))
plt.plot(arrest_trends_pd["ARREST_YEAR"] + arrest_trends_pd["ARREST_MONTH"] / 12, arrest_trends_pd["count"], marker='o')
plt.xlabel('Año')
plt.ylabel('Número de Arrestos')
plt.title('Tasas de Arrestos a lo Largo del Tiempo')
plt.grid(True)
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt

months = ['En', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ag', 'Sep', 'Oct', 'Nov', 'Dic']
arrests = [18817, 16744, 19036, 18303, 20198, 19490, 19158, 19893, 18461, 19920, 19150, 17702]

plt.figure(figsize=(10, 6))
plt.plot(months, arrests, marker='o')
plt.title('Tasas de Arrestos a lo Largo del Año 2023')
plt.xlabel('Meses')
plt.ylabel('Número de Arrestos')
plt.grid(True)
plt.show()


# COMMAND ----------

# MAGIC %md 9. ¿Qué distritos policiales (ARREST PRECINCT) o localidades (boroughs) (ARREST
# MAGIC BORO) tienen las mayores tasas de arrestos y cuáles son los tipos de delitos
# MAGIC predominantes en esas áreas?

# COMMAND ----------

# Obtener los recuentos de las diferentes categorías de ARREST_BORO
arrest_boro_counts = df1_cleaned.groupBy('ARREST_BORO').count().orderBy('count', ascending=False)

# Obtener los datos para graficar
arrest_boro = arrest_boro_counts.select('ARREST_BORO').rdd.map(lambda row: row[0]).collect()
counts = arrest_boro_counts.select('count').rdd.map(lambda row: row[0]).collect()

# Graficar
plt.figure(figsize=(10, 6))
plt.bar(arrest_boro, counts, color='skyblue')
plt.xlabel('Distrito de Arresto')
plt.ylabel('Cantidad de Arrestos')
plt.title('Distribución de Arrestos por Distrito')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

from pyspark.sql.window import Window

# Filtrar solo los delitos con códigos M, F y V
df_filtered = df1.filter(df1['LAW_CAT_CD'].isin(['M', 'F', 'V']))

# Contar la cantidad de arrestos por tipo de detención y municipio
detenciones_por_municipio = df_filtered.groupBy("ARREST_BORO", "LAW_CAT_CD").count()

# Definir la ventana de partición por municipio para calcular los tres delitos más comunes
window_spec = Window.partitionBy("ARREST_BORO").orderBy(F.desc("count"))

# Agregar un rango a cada fila dentro de cada partición (municipio) ordenado por la cantidad de arrestos
detenciones_por_municipio = detenciones_por_municipio.withColumn("rank", F.row_number().over(window_spec))

# Filtrar las filas con rango menor o igual a 3 para obtener los tres delitos más comunes en cada municipio
top_3_detenciones_por_municipio = detenciones_por_municipio.filter(F.col("rank") <= 3)

# Mostrar el resultado
top_3_detenciones_por_municipio.show()


# COMMAND ----------

df3_cleaned.printSchema()

# COMMAND ----------

# MAGIC %md 1. ¿Es frecuente que los autos tengan daños en lugares específicos
# MAGIC (VEHICLE_DAMAGE, VEHICLE_DAMAGE_1, VEHICLE_DAMAGE_2,
# MAGIC VEHICLE_DAMAGE_3) después de un accidente?
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt

# Obtener los 10 tipos de daños más frecuentes
top_vehicle_damage = df3_filtered.groupBy('VEHICLE_DAMAGE').count().orderBy('count', ascending=False).limit(10)

# Convertir los datos en una lista para el histograma
vehicle_damage_labels = top_vehicle_damage.select('VEHICLE_DAMAGE').rdd.flatMap(lambda x: x).collect()
vehicle_damage_counts = top_vehicle_damage.select('count').rdd.flatMap(lambda x: x).collect()

# Crear histograma para los 10 tipos de daños más frecuentes
plt.figure(figsize=(10, 6))
plt.bar(vehicle_damage_labels, vehicle_damage_counts, color='skyblue')
plt.title('Top 10 Tipos de Daños a Vehículos')
plt.xlabel('Tipo de Daño')
plt.ylabel('Frecuencia')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()



# COMMAND ----------

# MAGIC %md 1.¿Existe un patrón de accidentes relacionado con typos específicos de vehículos
# MAGIC (VEHICLE_TYPE)?

# COMMAND ----------

from pyspark.sql.functions import col, when

# Unir las filas que tienen variaciones en los valores de VEHICLE_TYPE
df3_cleaned = df3_cleaned.withColumn(
    'VEHICLE_TYPE',
    when(col('VEHICLE_TYPE').rlike('(?i)taxi'), 'TAXI')
    .when(col('VEHICLE_TYPE').rlike('(?i)Station Wagon|Sport Utility Vehicle|SPORT UTILITY/STATION WAGON'), 'SPORT UTILITY/STATION WAGON')
    .otherwise(col('VEHICLE_TYPE'))
)

# Filtrar valores nulos en VEHICLE_TYPE
df3_filtered = df3_cleaned.filter(df3_cleaned['VEHICLE_TYPE'].isNotNull())

# Obtener los recuentos de los tipos de vehículos
vehicle_type_counts = df3_filtered.groupBy('VEHICLE_TYPE').count().orderBy('count', ascending=False).limit(10)

# Obtener los datos para graficar
vehicle_types = vehicle_type_counts.select('VEHICLE_TYPE').rdd.map(lambda row: row[0]).collect()
counts = vehicle_type_counts.select('count').rdd.map(lambda row: row[0]).collect()

# Graficar
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.bar(vehicle_types, counts, color='skyblue')
plt.xlabel('Tipo de Vehículo')
plt.ylabel('Cantidad de Accidentes')
plt.title('Top 10 de Tipos de Vehículos Involucrados en Accidentes')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()




# COMMAND ----------

# MAGIC %md ¿Existe un patrón en las acciones que realizaban los vehículos (PRE_CRASH) justo
# MAGIC antes de los accidentes?
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt

# Obtener los 10 tipos de precrash más comunes
top_precrash = precrash_counts.orderBy(desc('count')).limit(10)

# Obtener los nombres de los tipos de precrash y sus frecuencias
precrash_names = [row['PRE_CRASH'] for row in top_precrash.collect()]
precrash_counts = [row['count'] for row in top_precrash.collect()]

# Crear el histograma
plt.figure(figsize=(10, 6))
plt.bar(precrash_names, precrash_counts, color='skyblue')
plt.title('Top 10 Tipos de Precrash Más Comunes')
plt.xlabel('Tipo de Precrash')
plt.ylabel('Frecuencia')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md 2.¿Los vehículos involucrados en accidentes presentan patrones
# MAGIC específicos en las fechas y horas de los choques (CRASH DATE, CRASH TIME) que
# MAGIC podrían ayudar a prevenir futuros incidentes?
# MAGIC
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt
# Contar el número de accidentes por año
crash_counts = df3_cleaned.groupBy('CRASH_YEAR').count().orderBy('CRASH_YEAR')

# Convertir el DataFrame de Spark a pandas
crash_counts_pd = crash_counts.toPandas()

# Crear el histograma
plt.figure(figsize=(12, 6))
plt.bar(crash_counts_pd['CRASH_YEAR'], crash_counts_pd['count'], color='skyblue', edgecolor='black')
plt.title('Histograma de Accidentes por Año')
plt.xlabel('Año de Accidente')
plt.ylabel('Frecuencia')
plt.xticks(rotation=45)  # Rotar etiquetas del eje x para mejor legibilidad
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Mostrar el histograma
plt.show()


# COMMAND ----------

from pyspark.sql.functions import hour

# 1. Relación entre momentos del día de los accidentes y la frecuencia
# Crear una columna para la hora del día a partir de la columna CRASH_TIME
df3 = df3_cleaned.withColumn('CRASH_HOUR', hour(df3_cleaned['CRASH_TIME']))

# Histograma de la distribución de los momentos del día de los accidentes (CRASH_HOUR)
plt.figure(figsize=(10, 6))
sns.histplot(data=df3.toPandas(), x='CRASH_HOUR', bins=24, kde=False)
plt.title('Distribución de accidentes por momento del día')
plt.xlabel('Hora del día')
plt.ylabel('Frecuencia de accidentes')
plt.xticks(range(0, 24))
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Logistic Regression
# MAGIC Justificación Técnica: Aunque más simple que los modelos de ensamble, la regresión logística es fácil de implementar, interpretar, y escalar. Es efectiva para establecer una línea base y para interpretar la influencia relativa de cada predictor gracias a sus coeficientes modelados.
# MAGIC
# MAGIC Justificación de Negocio: La claridad y la facilidad de interpretación que ofrece la regresión logística son de gran valor cuando los resultados del modelo deben ser comunicados a stakeholders no técnicos o utilizados para justificar decisiones basadas en datos ante el público o entidades reguladoras. La capacidad de explicar cómo y por qué se hacen ciertas predicciones es fundamental en contextos sensibles como el análisis racial.
# MAGIC
# MAGIC
# MAGIC <h2>Aprendizaje No Supervisado: K-Means</h2>
# MAGIC
# MAGIC
# MAGIC <h3>Justificación:</h3>
# MAGIC
# MAGIC
# MAGIC Los datos incluyen características numéricas como ARREST_PRECINCT, Latitude, Longitude, ARREST_YEAR y ARREST_MONTH, que pueden ser utilizadas para agrupar los datos en clústeres.
# MAGIC
# MAGIC
# MAGIC K-Means es un algoritmo de agrupamiento ampliamente utilizado que puede identificar patrones y segmentos dentro de los datos sin la necesidad de etiquetas de clase.
# MAGIC
# MAGIC
# MAGIC Puede ayudar a identificar grupos de observaciones con características similares, lo que podría ser útil para comprender la estructura de los datos y encontrar segmentos significativos.

# COMMAND ----------

# MAGIC %md 
# MAGIC <h1> Preparación de datos para modelado

# COMMAND ----------

# MAGIC %md
# MAGIC #Datos de arrestos del Departamento de Policía de Nueva York hasta la fecha

# COMMAND ----------

# MAGIC %md 
# MAGIC <h2> Eliminar características fuertemente correlacionada

# COMMAND ----------

df1_cleaned.printSchema()

# COMMAND ----------

display (df1_cleaned)

# COMMAND ----------

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

# Seleccionar solo las variables numéricas
variables_numericas = df1_cleaned.select( "Latitude", "Longitude","ARREST_PRECINCT","ARREST_MONTH")

# Convertir las variables a un solo vector
ensamblador = VectorAssembler(inputCols=variables_numericas.columns, outputCol="features")
variables_ensambladas = ensamblador.transform(variables_numericas)

# Calcular la matriz de correlación
matriz_correlacion = Correlation.corr(variables_ensambladas, "features").head()

# Extraer la matriz de correlación
matriz_correlacion = matriz_correlacion[0].toArray()

# COMMAND ----------

import seaborn as sns

# Convertir la matriz de correlación en un DataFrame de pandas para usar con seaborn
matriz_correlacion_df = pd.DataFrame(matriz_correlacion, columns=variables_numericas.columns, index=variables_numericas.columns)

# Crear un mapa de calor de la matriz de correlación
plt.figure(figsize=(10, 8))
sns.heatmap(matriz_correlacion_df, annot=True, cmap='coolwarm', linewidths=0.5)
plt.title('Matriz de Correlación')
plt.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC <h2> Normalización utilizando StandardScaler

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import col

# Seleccionar solo las columnas numéricas
numeric_columns = ["Latitude", "Longitude","ARREST_PRECINCT","ARREST_MONTH","ARREST_YEAR"]

# Crear un VectorAssembler para combinar las columnas numéricas en un solo vector
assembler = VectorAssembler(inputCols=numeric_columns, outputCol="numeric_features")

# Crear un StandardScaler para normalizar los datos
scaler = StandardScaler(inputCol="numeric_features", outputCol="scaled_features", withMean=True, withStd=True)

# Ajustar y transformar los datos en una sola etapa usando el Pipeline
df1_assembled = assembler.transform(df1_cleaned)
scaler_model = scaler.fit(df1_assembled)
df1_scaled = scaler_model.transform(df1_assembled)

# Reemplazar las columnas numéricas originales con las normalizadas
# Primero, asegurémonos de que scaled_features es un vector de columnas
from pyspark.ml.functions import vector_to_array

# Convertir la columna de vectores en un array
df1_scaled = df1_scaled.withColumn("scaled_features_array", vector_to_array(col("scaled_features")))

# Ahora reemplazar cada columna numérica original con su valor escalado
for i, col_name in enumerate(numeric_columns):
    df1_scaled = df1_scaled.withColumn(col_name, col("scaled_features_array")[i])

# Eliminar las columnas temporales
df1_scaled = df1_scaled.drop("numeric_features", "scaled_features", "scaled_features_array")

# Mostrar las primeras filas del DataFrame resultante con las características normalizadas
df1_scaled.select(numeric_columns).show(truncate=False)




# COMMAND ----------

df1_scaled.printSchema()

# COMMAND ----------

display (df1_scaled)

# COMMAND ----------

# MAGIC %md
# MAGIC #Colisiones de vehículos motorizados - Vehículos

# COMMAND ----------

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# Seleccionar solo las variables numéricas
variables_numericas = df3_cleaned.select("CRASH_HOUR", "CRASH_YEAR", "CRASH_MONTH")

# Convertir las variables a un solo vector
ensamblador = VectorAssembler(inputCols=variables_numericas.columns, outputCol="features")
variables_ensambladas = ensamblador.transform(variables_numericas).select("features")

# Calcular la matriz de correlación
matriz_correlacion = Correlation.corr(variables_ensambladas, "features").head()[0]

# Convertir la matriz de correlación a un array numpy y luego a un DataFrame de Pandas
correlacion = matriz_correlacion.toArray()
df_correlacion = pd.DataFrame(correlacion, index=variables_numericas.columns, columns=variables_numericas.columns)

# Crear el mapa de calor utilizando Seaborn
plt.figure(figsize=(10, 8))
sns.heatmap(df_correlacion, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
plt.title('Matriz de Correlación')
plt.show()



# COMMAND ----------

# MAGIC %md 
# MAGIC <h2> Normalización utilizando StandardScaler

# COMMAND ----------

from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, DoubleType

# Seleccionar las columnas numéricas a estandarizar
numeric_columns = ['CRASH_HOUR', 'CRASH_YEAR', 'CRASH_MONTH']

# Crear un VectorAssembler para combinar las columnas numéricas en una sola columna de vector
assembler = VectorAssembler(inputCols=numeric_columns, outputCol="numeric_features")

# Transformar los datos utilizando el VectorAssembler
df_assembled = assembler.transform(df3_cleaned)

# Crear un StandardScaler para escalar los datos
scaler = StandardScaler(inputCol="numeric_features", outputCol="scaled_features", withMean=True, withStd=True)

# Ajustar el modelo de StandardScaler
scaler_model = scaler.fit(df_assembled)

# Transformar los datos con el StandardScaler
df3_scaled = scaler_model.transform(df_assembled)

# Convertir los vectores a una lista de valores
to_array_udf = udf(lambda v: v.toArray().tolist(), ArrayType(DoubleType()))
df3_scaled = df3_scaled.withColumn("scaled_features_array", to_array_udf(col("scaled_features")))

# Reemplazar los valores originales con los valores escalados
for i, col_name in enumerate(numeric_columns):
    df3_scaled = df3_scaled.withColumn(col_name, col("scaled_features_array")[i])

# Eliminar las columnas temporales 'numeric_features', 'scaled_features' y 'scaled_features_array'
df3_scaled = df3_scaled.drop("numeric_features").drop("scaled_features").drop("scaled_features_array")

# Mostrar las primeras filas para verificar
df3_scaled.select(numeric_columns).show()


# COMMAND ----------

# MAGIC %md 
# MAGIC <h2>Selección de variables según criterio de negocio

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Variable escogida raza del detenido (PERP_RACE)</h3>
# MAGIC
# MAGIC La selección de la variable "raza del detenido" como variable objetivo se fundamenta en la importancia de comprender y abordar posibles disparidades raciales en el sistema de justicia penal. Esta variable nos permite investigar y analizar la posible influencia de factores raciales en el proceso de arresto, lo que puede arrojar luz sobre posibles sesgos o inequidades en la aplicación de la ley.
# MAGIC
# MAGIC
# MAGIC Explorar la relación entre la raza de los individuos detenidos y otras variables predictoras nos permite identificar patrones y tendencias que podrían indicar desigualdades sistemáticas en la aplicación de la ley. Además, al comprender cómo la raza puede estar relacionada con el riesgo de arresto o el tipo de cargos enfrentados, podemos avanzar hacia estrategias más equitativas y justas en el sistema de justicia penal.
# MAGIC
# MAGIC
# MAGIC Al centrarnos en la variable de raza del detenido, buscamos no solo cuantificar la prevalencia de arrestos entre diferentes grupos raciales, sino también examinar las posibles causas subyacentes de las disparidades observadas. Este enfoque nos brinda la oportunidad de promover una mayor transparencia y rendición de cuentas en el sistema de justicia penal y de trabajar hacia una aplicación más justa y equitativa de la ley para todos los ciudadanos.

# COMMAND ----------

# MAGIC %md
# MAGIC # Tecnicas de Machine Learning con ML Lib

# COMMAND ----------

df1_cleaned.printSchema()

# COMMAND ----------

df1_cleaned.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aprendizaje Supervisado con Regresión Logística
# MAGIC
# MAGIC - **Preprocesamiento**: Indexación de la variable categórica `PERP_RACE` y ensamblaje de las características numéricas.
# MAGIC - **División de Datos**: Conjuntos de entrenamiento y prueba (70/30).
# MAGIC - **Configuración de Modelo**: Variación del parámetro de regularización para observar su impacto en la precisión del modelo.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

# Indexar la variable categórica objetivo
indexer = StringIndexer(inputCol="PERP_RACE", outputCol="label").fit(df1_scaled)
df_indexed = indexer.transform(df1_scaled)

# Asamblea de features numéricas
feature_cols = ['ARREST_PRECINCT', 'Latitude', 'Longitude', 'ARREST_YEAR', 'ARREST_MONTH']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_transformed = assembler.transform(df_indexed)

# Dividir los datos en conjuntos de entrenamiento y prueba
(train_data, test_data) = df_transformed.randomSplit([0.7, 0.3], seed=42)

# Definir y entrenar el modelo de regresión logística con diferentes parámetros
reg_params = [0.01, 0.1, 1.0]  # Diferentes parámetros de regularización
results = []

for reg in reg_params:
    lr = LogisticRegression(featuresCol="features", labelCol="label", regParam=reg)
    lr_model = lr.fit(train_data)
    predictions = lr_model.transform(test_data)

    # Evaluar el modelo
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    results.append((reg, accuracy))

    # Matriz de confusión
    rdd = predictions.select(['prediction', 'label']).rdd.map(tuple)
    metrics = MulticlassMetrics(rdd)
    print("Confusion Matrix for regParam = {}:".format(reg))
    print(metrics.confusionMatrix().toArray())

# Graficar los resultados
regs, accs = zip(*results)
plt.figure(figsize=(10, 5))
plt.plot(regs, accs, marker='o')
plt.xlabel('Regularization Parameter')
plt.ylabel('Accuracy')
plt.title('Accuracy vs. Regularization Parameter')
plt.grid(True)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insights de la Regresión Logística
# MAGIC
# MAGIC ### 1. **Impacto del Parámetro de Regularización en la Precisión**
# MAGIC - La gráfica "Accuracy vs. Regularization Parameter" muestra una disminución drástica en la precisión al aumentar el valor del parámetro de regularización desde 0.01 a 0.1 y 1.0.
# MAGIC - La precisión máxima se alcanza con el parámetro de regularización más bajo (0.01), lo que indica que un nivel menor de regularización es preferible para este conjunto de datos.
# MAGIC
# MAGIC ### 2. **Análisis de las Matrices de Confusión**
# MAGIC - **Para `regParam = 0.01`**: Aunque la mayoría de las predicciones se concentran en la primera clase, hay una pequeña cantidad de predicciones dispersas entre otras clases. Esto sugiere que el modelo tiene cierta capacidad para distinguir entre clases, pero aún es predominantemente inclinado hacia la clase mayoritaria.
# MAGIC - **Para `regParam = 0.1` y `1.0`**: Las matrices de confusión muestran que todas las predicciones se asignan a la primera clase, indicando que el modelo se ha vuelto completamente incapaz de distinguir entre clases a medida que aumenta la regularización.
# MAGIC
# MAGIC ### 3. **Desbalance de Clases**
# MAGIC - Las matrices de confusión revelan un claro problema de desbalance de clases, donde el modelo tiende a predecir principalmente la clase con más muestras (primera clase). Esto es especialmente evidente con valores de regularización más altos.
# MAGIC
# MAGIC ### 4. **Necesidad de Estrategias de Balanceo de Clases**
# MAGIC - Para mejorar el rendimiento del modelo y su capacidad para clasificar correctamente todas las clases, sería beneficioso implementar técnicas de balanceo de clases como sobremuestreo de las clases minoritarias o submuestreo de la clase mayoritaria.
# MAGIC
# MAGIC ### 5. **Exploración de Modelos Alternativos o Mejoras**
# MAGIC - Dado que la regresión logística muestra una limitada capacidad para manejar el desbalance de clases en este escenario, podría ser útil explorar modelos más robustos ante este problema, como Random Forest o Boosting, que generalmente manejan mejor el desbalance.
# MAGIC
# MAGIC ### 6. **Ajuste Fino de Hiperparámetros**
# MAGIC - Además de ajustar el parámetro de regularización, explorar otros hiperparámetros como el `elasticNetParam` (para regularización L1 y L2 combinada) y el `maxIter` (número de iteraciones), podría proporcionar mejoras en la precisión y la capacidad de generalización del modelo.
# MAGIC
# MAGIC ## Conclusión
# MAGIC Los resultados sugieren que la regresión logística, con la configuración actual y el desbalance significativo en las clases, no es suficientemente efectiva para predecir la variable `PERP_RACE` con alta precisión y equidad entre las clases. Es recomendable investigar otras técnicas y ajustes para desarrollar un modelo más robusto y justo.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aprendizaje No Supervisado con K-Means Clustering
# MAGIC
# MAGIC - **Preprocesamiento**: Normalización de las características numéricas para asegurar una escala uniforme.
# MAGIC - **Configuración de Modelo**: Pruebas con diferentes números de clústeres para encontrar la configuración óptima.
# MAGIC - **Evaluación**: Uso del score de silueta para determinar la calidad del clustering.

# COMMAND ----------

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
import matplotlib.pyplot as plt
import seaborn as sns

# Asegurarse de que los datos están transformados
df_transformed = assembler.transform(df1_scaled)

# Prueba de diferentes números de clústeres
cluster_counts = [2, 3, 5, 7]
silhouettes = []

for k in cluster_counts:
    kmeans = KMeans(featuresCol="features", k=k, seed=42)
    model = kmeans.fit(df_transformed)
    predictions = model.transform(df_transformed)

    # Evaluar el modelo
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    silhouettes.append(silhouette)
    print("Silhouette score for k={} is {}".format(k, silhouette))

    # Realizar PCA para reducir a 2 dimensiones
    pca = PCA(k=2, inputCol="features", outputCol="pcaFeatures")
    model_pca = pca.fit(predictions)  # Asegurarse de usar 'predictions'
    result_pca = model_pca.transform(predictions)

    # Extraer las coordenadas PCA y las etiquetas de clúster para graficar
    pca_features = result_pca.select("pcaFeatures", "prediction").collect()
    x_pca = [feat.pcaFeatures[0] for feat in pca_features]
    y_pca = [feat.pcaFeatures[1] for feat in pca_features]
    cluster_labels = [feat.prediction for feat in pca_features]

    # Graficar los resultados de PCA con las etiquetas de clúster
    plt.figure(figsize=(10, 7))
    sns.scatterplot(x=x_pca, y=y_pca, hue=cluster_labels, palette="viridis", legend='full', s=100)
    plt.title(f'PCA Plot of Clusters for k={k}')
    plt.xlabel('Principal Component 1')
    plt.ylabel('Principal Component 2')
    plt.legend(title='Cluster')
    plt.show()

# Graficar Silhouette Scores
plt.figure(figsize=(10, 5))
plt.plot(cluster_counts, silhouettes, marker='o')
plt.xlabel('Number of Clusters')
plt.ylabel('Silhouette Score')
plt.title('Silhouette Score vs. Number of Clusters')
plt.grid(True)
plt.show()



# COMMAND ----------

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Asumimos que 'df1_scaled' es tu DataFrame original que incluye la columna 'PERP_RACE'

# Indexar la variable categórica y contar el número de categorías únicas
indexer = StringIndexer(inputCol="PERP_RACE", outputCol="raceIndex")
df_indexed = indexer.fit(df1_scaled).transform(df1_scaled)
K = df_indexed.select("raceIndex").distinct().count()  # Aquí definimos 'K' correctamente

# Asamblea de features
feature_cols = ['ARREST_PRECINCT', 'Latitude', 'Longitude', 'ARREST_YEAR', 'ARREST_MONTH']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_transformed = assembler.transform(df_indexed)

# Configurar K-Means con el número de categorías como clústeres
kmeans = KMeans(featuresCol="features", k=K, seed=42)
model = kmeans.fit(df_transformed)
predictions = model.transform(df_transformed)

# Evaluar el modelo usando el score de silueta
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette score for {K} clusters: {silhouette}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Insights Generales del Modelo K-Means
# MAGIC
# MAGIC ### 1. **Diferenciación Clara en Clústeres Pequeños (k=2)**
# MAGIC - El Silhouette score casi perfecto con dos clústeres (`0.99998`) sugiere que hay dos grupos muy distintos en tus datos. Esto puede ser extremadamente útil si los objetivos de negocio implican diferenciar entre dos categorías principales, como clientes nuevos versus recurrentes, productos de alto valor versus bajo valor, o cualquier otro binario relevante para la toma de decisiones.
# MAGIC
# MAGIC ### 2. **Aumento de Complejidad con Más Clústeres**
# MAGIC - A medida que aumenta el número de clústeres, el Silhouette score disminuye, indicando una separación menos clara entre los grupos. Sin embargo, el incremento de score con `k=7` comparado con `k=3` y `k=5` podría reflejar una estructura más compleja en los datos que podría ser explotada para obtener insights más granulares.
# MAGIC
# MAGIC ### 3. **Implicaciones para la Segmentación de Clientes**
# MAGIC - En un contexto de negocio, especialmente en marketing y desarrollo de productos, entender estas agrupaciones puede facilitar la personalización de estrategias para diferentes segmentos de clientes. Por ejemplo, los clústeres pueden identificar grupos con patrones de consumo distintos, lo que permite diseñar ofertas y comunicaciones más dirigidas.
# MAGIC
# MAGIC ## Recomendaciones Estratégicas y Operativas
# MAGIC
# MAGIC ### 1. **Optimización de Recursos**
# MAGIC - Utilizar el modelo con `k=2` para operaciones donde la distinción clara es crítica puede optimizar los recursos, concentrando esfuerzos en dos grandes estrategias diferenciadas.
# MAGIC - Para investigaciones más profundas o campañas de marketing específicas, utilizar un número mayor de clústeres (como `k=7`) podría permitir una aproximación más matizada.
# MAGIC
# MAGIC ### 2. **Desarrollo de Productos y Ofertas**
# MAGIC - Los insights derivados de los diferentes clústeres pueden guiar el desarrollo de productos o servicios adaptados a las necesidades y preferencias de cada grupo. Esto es especialmente relevante en sectores como el financiero, donde diferentes clústeres podrían indicar diferentes niveles de riesgo y preferencias de inversión.
# MAGIC
# MAGIC ### 3. **Mejora Continua y Aprendizaje Organizacional**
# MAGIC - Los clústeres identificados y su evolución en el tiempo pueden ser monitorizados para entender mejor las tendencias del mercado y los cambios en las preferencias de los consumidores. Este aprendizaje continuo puede alimentar la innovación y la adaptabilidad organizacional.
# MAGIC
# MAGIC ### 4. **Validación y Refinamiento del Modelo**
# MAGIC - Dada la variabilidad en los Silhouette scores, es recomendable realizar una validación cruzada de los clústeres con otras métricas y, posiblemente, con un análisis cualitativo. Además, considerar la implementación de otros métodos de clustering o análisis multivariable para confirmar o complementar los hallazgos.
# MAGIC
# MAGIC ## Consideraciones Finales
# MAGIC
# MAGIC - **Evaluación de Impacto**: Cualquier cambio basado en estos clústeres debe ser cuidadosamente evaluado respecto a su impacto potencial en los resultados del negocio.
# MAGIC - **Uso Ético de los Datos**: Asegurar que la segmentación y personalización basadas en clústeres se manejen de manera ética, respetando la privacidad de los individuos y evitando sesgos indebidos.
# MAGIC
# MAGIC
# MAGIC
