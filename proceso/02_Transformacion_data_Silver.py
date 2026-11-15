# Databricks notebook source

#Trasnsformar campos, campos y limpieza de información

from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOGO = "smartdata_jeol"
ORIGEN = "bronze"
DESTINO = "silver"



df_autos = spark.table("smartdata_jeol.bronze.automoviles")

df_movies = spark.table("smartdata_jeol.bronze.movies")

df_film_details = spark.table("smartdata_jeol.bronze.film_details")

df_more_info = spark.table("smartdata_jeol.bronze.more_info")

df_poster_path = spark.table("smartdata_jeol.bronze.poster_path")

# Revisar estructura
df_autos.printSchema()

# Mostrar los primeros registros
display(df_autos.limit(10))


# COMMAND ----------

from pyspark.sql.functions import col, count, when

# Contar valores nulos por columna
df_autos.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_autos.columns
]).show()


# COMMAND ----------


# Contar valores '?' en columnas de texto

columnas_texto = [
    c for c, tipo in df_autos.dtypes
    if tipo == "string"
]

df_autos.select([
    count(when(col(c) == "?", c)).alias(c)
    for c in columnas_texto
]).show()


# COMMAND ----------


df_movies.printSchema()

display(df_movies.limit(10))

# Contar valores nulos
df_movies.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_movies.columns
]).show()


# COMMAND ----------


from pyspark.sql.functions import col, count, when, trim

# Analizar calidad de datos
def revisar_datos(df):

    print("Total de registros:", df.count())
    print("Total de columnas:", len(df.columns))

    for campo in df.columns:

        nulos = df.filter(col(campo).isNull()).count()

        interrogantes = df.filter(
            trim(col(campo).cast("string")) == "?"
        ).count()

        if nulos > 0 or interrogantes > 0:
            print(campo,
                  "| Nulos:", nulos,
                  "| Valores ?:", interrogantes)

# Revisar automóviles
revisar_datos(df_autos)


# COMMAND ----------


revisar_datos(df_movies)
revisar_datos(df_film_details)
revisar_datos(df_more_info)
revisar_datos(df_poster_path)

# COMMAND ----------


from pyspark.sql.functions import regexp_replace

# Crear dataframe Silver
df_autos_silver = df_autos

# Reemplazar '?' por valores nulos
for campo in df_autos_silver.columns:

    if dict(df_autos_silver.dtypes)[campo] == "string":

        df_autos_silver = df_autos_silver.withColumn(
            campo,
            when(trim(col(campo)) == "?", None)
            .otherwise(col(campo))
        )

display(df_autos_silver.limit(10))


# COMMAND ----------



# Columnas que deben ser numéricas
columnas_numericas = [
    "normalized-losses", #Campo1
    "bore", #Campo2
    "stroke", #Campo3
    "horsepower", #Campo4
    "peak-rpm", #Campo5
    "price" #Campo6
]

# Convertir a tipo numérico
for campo in columnas_numericas:

    df_autos_silver = df_autos_silver.withColumn(
        campo,
        col(campo).cast("double")
    )

# Verificar resultados
df_autos_silver.printSchema()



# COMMAND ----------


from pyspark.sql.functions import col, count, when

# Contar valores nulos por columna
df_autos_silver.select([
    count(when(col(campo).isNull(), campo)).alias(campo)
    for campo in df_autos_silver.columns
]).show(truncate=False)

# COMMAND ----------

 #Limpieoza de información
#reemplazar nulls
promedio = df_autos_silver.selectExpr(
    "avg(`normalized-losses`)"
).first()[0]

df_autos_silver = df_autos_silver.fillna({
    "normalized-losses": promedio
})

# Reemplazar nulos de puertas por "four"
df_autos_silver = df_autos_silver.fillna({
    "num-of-doors": "four"
})

# Mostrar resultados
display(df_autos_silver.limit(10))

# COMMAND ----------


df_autos_silver = df_autos_silver.fillna({
    "num-of-doors": "four"
})



# COMMAND ----------


# Contar registros originales
total = df_autos_silver.count()

# Contar registros sin duplicados
unicos = df_autos_silver.dropDuplicates().count()

print("Total registros:", total)
print("Registros únicos:", unicos) 

# COMMAND ----------


# Guardar tabla Silver
df_autos_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("smartdata_jeol.silver.automoviles")

print("Tabla Silver creada correctamente")



# COMMAND ----------


# Revisar valores nulos por columna
from pyspark.sql.functions import col

for campo in df_autos_silver.columns:
    nulos = df_autos_silver.filter(col(campo).isNull()).count()

    if nulos > 0:
        print(campo, ":", nulos)

# COMMAND ----------


# PASO 18: Reemplazar valores nulos con el promedio

columnas = ["bore", "stroke", "horsepower", "peak-rpm", "price"]

for campo in columnas:

    promedio = df_autos_silver.selectExpr(
        f"avg(`{campo}`)"
    ).first()[0]

    df_autos_silver = df_autos_silver.fillna({
        campo: promedio
    })

print("Valores nulos reemplazados correctamente")

# COMMAND ----------


from pyspark.sql.functions import col

for campo in df_autos_silver.columns:

    nulos = df_autos_silver.filter(
        col(campo).isNull()
    ).count()

    if nulos > 0:
        print(campo, ":", nulos)

print("Validación finalizada")

# COMMAND ----------

df_autos_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("smartdata_jeol.silver.automoviles")

print("Tabla Silver creada correctamente")

# COMMAND ----------


# Leer la tabla Silver
df = spark.table("smartdata_jeol.silver.automoviles")

print("Total de registros:", df.count())


# COMMAND ----------


# PASO 22: Leer tablas de películas

df_movies = spark.table("smartdata_jeol.bronze.movies")

df_film_details = spark.table("smartdata_jeol.bronze.film_details")

df_more_info = spark.table("smartdata_jeol.bronze.more_info")

df_poster_path = spark.table("smartdata_jeol.bronze.poster_path")

print("Tablas de películas cargadas correctamente")


# COMMAND ----------


print("TABLA MOVIES")

display(df_movies.limit(5))

print("TABLA FILM DETAILS")

display(df_film_details.limit(5))

print("TABLA MORE INFO")

display(df_more_info.limit(5))

print("TABLA POSTER PATH")


display(df_poster_path.limit(5))

# COMMAND ----------

# PASO 24: Revisar valores nulos

tablas = {
    "MOVIES": df_movies,
    "FILM DETAILS": df_film_details,
    "MORE INFO": df_more_info,
    "POSTER PATH": df_poster_path
}

for nombre, df in tablas.items():

    print("TABLA:", nombre)
    print("Total registros:", df.count())

    for campo in df.columns:
        nulos = df.filter(df[campo].isNull()).count()

        if nulos > 0:
            print(campo, ":", nulos)

    print("----------------------")


# COMMAND ----------

for nombre, df in tablas.items():

    total = df.count()
    unicos = df.select("id").distinct().count()

    print("Tabla:", nombre)
    print("Duplicados:", total - unicos)
    print("--------------------")


# COMMAND ----------


# PASO 27: Guardar tablas Silver

tablas_silver = {
    "movies": df_movies,
    "film_details": df_film_details,
    "more_info": df_more_info,
    "poster_path": df_poster_path
}

for nombre, df in tablas_silver.items():

    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("smartdata_jeol.silver." + nombre)

    print("Tabla guardada:", nombre)
