# Databricks notebook source
 #Conexiones para importar librerias, cadenas y campops
from pyspark.sql import functions as F

# Catálogo y esquema de destino
CATALOGO = "smartdata_jeol"
ESQUEMA_BRONZE = "bronze"

# ruta del container raw
RUTA_RAW = (
    "abfss://raw@adlsmartdatajeol13"
    ".dfs.core.windows.net/"
)

# Dataset 1: Automóviles
RUTA_AUTOMOVILES = RUTA_RAW + "automoviles/Automobile_data.csv"

# Dataset 2: Películas
RUTA_PELICULAS = RUTA_RAW + "peliculas/"

print("Catálogo:", CATALOGO)
print("Esquema:", ESQUEMA_BRONZE)
print("Ruta automóviles:", RUTA_AUTOMOVILES)
print("Ruta películas:", RUTA_PELICULAS)

# COMMAND ----------

# Validar la info desde ADLS Gen2

df_autos = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RUTA_AUTOMOVILES)
)

print("Registros:", df_autos.count())
print("Columnas:", len(df_autos.columns))

display(df_autos.limit(10))


# COMMAND ----------


#Leer data-sets

archivos_peliculas = {
    "Movies": "Movies.csv",
    "FilmDetails": "FilmDetails.csv",
    "MoreInfo": "MoreInfo.csv",
    "PosterPath": "PosterPath.csv"
}

df_peliculas = {}

for nombre, archivo in archivos_peliculas.items():

    ruta = RUTA_PELICULAS + archivo

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ruta)
    )

    df_peliculas[nombre] = df

    print(f"Archivo: {archivo}")
    print(f"Registros: {df.count()}")
    print(f"Columnas: {len(df.columns)}")
    print("-" * 40)


# COMMAND ----------


# =====================================================
# DATASET 2: PELÍCULAS
# Lectura de archivos CSV desde ADLS Gen2
# =====================================================

archivos_peliculas = {
    "Movies": "Movies.csv",
    "FilmDetails": "FilmDetails.csv",
    "MoreInfo": "MoreInfo.csv",
    "PosterPath": "PosterPath.csv"
}

# Diccionario para almacenar DataFrames
df_peliculas = {}

for nombre, archivo in archivos_peliculas.items():

    ruta = RUTA_PELICULAS + archivo

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ruta)
    )

    df_peliculas[nombre] = df

    print(f"Archivo: {archivo}")
    print(f"Registros: {df.count()}")
    print(f"Columnas: {len(df.columns)}")
    print("-" * 45)


# COMMAND ----------


# =====================================================
# PASO 4: ELIMINAR Y RECREAR TABLAS BRONZE
# PROYECTO: DataSet_Automoviles_y_Peliculas
# =====================================================

from pyspark.sql.functions import current_timestamp, lit

CATALOGO = "smartdata_jeol"
ESQUEMA = "bronze"

# 1. DataFrames y tablas de destino
tablas_bronze = {
    "automoviles": (df_autos, "Automobile_data.csv"),
    "movies": (df_peliculas["Movies"], "Movies.csv"),
    "film_details": (df_peliculas["FilmDetails"], "FilmDetails.csv"),
    "more_info": (df_peliculas["MoreInfo"], "MoreInfo.csv"),
    "poster_path": (df_peliculas["PosterPath"], "PosterPath.csv")
}

# 2. Eliminar y recrear tablas
for nombre_tabla, (df, archivo) in tablas_bronze.items():

    tabla_destino = f"{CATALOGO}.{ESQUEMA}.{nombre_tabla}"

    print(f"Procesando: {tabla_destino}")

    # Eliminar tabla existente
    spark.sql(f"DROP TABLE IF EXISTS {tabla_destino}")

    # Agregar campos de seguimiento o
    df_bronze = (
        df
        .withColumn("fecha_insert", current_timestamp())
        .withColumn("archivo_origen", lit(archivo))
    )

    # creaciión tablas 
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(tabla_destino)
    )

    print(f"Tabla creada: {tabla_destino}")
    print("-" * 50)

print("Proceso Bronze finalizado correctamente.")


# COMMAND ----------

for nombre_tabla in tablas_bronze:

    tabla = f"smartdata_jeol.bronze.{nombre_tabla}"

    total = spark.table(tabla).count()

    print(f"{nombre_tabla}: {total} registros")


# COMMAND ----------


spark.sql("""
    SHOW TABLES IN smartdata_jeol.bronze
""").show(truncate=False)
