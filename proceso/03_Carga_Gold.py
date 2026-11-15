# Databricks notebook source
 #Lectura de documentos

df_autos = spark.table("smartdata_jeol.silver.automoviles")

df_movies = spark.table("smartdata_jeol.silver.movies")

df_film_details = spark.table("smartdata_jeol.silver.film_details")

df_more_info = spark.table("smartdata_jeol.silver.more_info")

df_poster_path = spark.table("smartdata_jeol.silver.poster_path")

print("Tablas Silver cargadas correctamente")


# COMMAND ----------

print("MOVIES:", df_movies.columns)

print("FILM DETAILS:", df_film_details.columns)

print("MORE INFO:", df_more_info.columns)

print("POSTER PATH:", df_poster_path.columns)

# COMMAND ----------

 
df_peliculas_gold = df_movies \
    .join(
        df_film_details.select(
            "id", "director", "top_billed",
            "budget_usd", "revenue_usd"
        ),
        "id", "left"
    ) \
    .join(
        df_more_info.select(
            "id", "runtime", "budget", "revenue", "film_id"
        ),
        "id", "left"
    ) \
    .join(
        df_poster_path.select(
            "id", "poster_path", "backdrop_path"
        ),
        "id", "left"
    )

print("Tablas unidas correctamente")
print("Total de películas:", df_peliculas_gold.count())

display(df_peliculas_gold.limit(10))


# COMMAND ----------

print("Películas originales:", df_movies.count())
print("Películas integradas:", df_peliculas_gold.count())

# COMMAND ----------

 

df_peliculas_gold.filter(
    "film_id IS NOT NULL AND id != film_id"
).select(
    "id", "film_id", "title"
).show(10)

print("Registros con ID diferente:",
    df_peliculas_gold.filter(
        "film_id IS NOT NULL AND id != film_id"
    ).count()
)


# COMMAND ----------


df_peliculas_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("smartdata_jeol.gold.peliculas")

print("Tabla Gold de películas creada correctamente")


# COMMAND ----------


# Verificar tabla Gold
df = spark.table("smartdata_jeol.gold.peliculas")

print("Total de películas:", df.count())

display(df.limit(10))


# COMMAND ----------


from pyspark.sql.functions import avg, count, round

# Leer tabla Silver
df_autos = spark.table("smartdata_jeol.silver.automoviles")

# Agrupar automóviles por marca
df_autos_gold = df_autos.groupBy("make").agg(
    count("*").alias("total_automoviles"),
    round(avg("price"), 2).alias("precio_promedio"),
    round(avg("horsepower"), 2).alias("potencia_promedio")
)

# Mostrar resultados
display(df_autos_gold)


# COMMAND ----------


df_autos_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("smartdata_jeol.gold.automoviles_resumen")

print("Tabla Gold de automóviles creada correctamente")

# COMMAND ----------


df_peliculas = spark.table("smartdata_jeol.gold.peliculas")

df_automoviles = spark.table("smartdata_jeol.gold.automoviles_resumen")

print("Total de películas:", df_peliculas.count())
print("Total de marcas:", df_automoviles.count())

print("RESUMEN DE AUTOMÓVILES")
display(df_automoviles)


# COMMAND ----------

 
df_peliculas = spark.table("smartdata_jeol.gold.peliculas")

df_autos = spark.table("smartdata_jeol.gold.automoviles_resumen")

print("Total de películas:", df_peliculas.count())
print("Total de marcas:", df_autos.count())
