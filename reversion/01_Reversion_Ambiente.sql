 
-- IMPORTANTE:
-- Ejecutar solamente cuando se requiera
-- eliminar los objetos del proyecto.
-- NO ejecutar durante la operación normal.

-- 1. ELIMINAR TABLAS GOLD

DROP TABLE IF EXISTS smartdata_jeol.gold.peliculas;

DROP TABLE IF EXISTS smartdata_jeol.gold.automoviles_resumen;


-- 2. ELIMINAR TABLAS SILVER

DROP TABLE IF EXISTS smartdata_jeol.silver.automoviles;

DROP TABLE IF EXISTS smartdata_jeol.silver.movies;

DROP TABLE IF EXISTS smartdata_jeol.silver.film_details;

DROP TABLE IF EXISTS smartdata_jeol.silver.more_info;

DROP TABLE IF EXISTS smartdata_jeol.silver.poster_path;


-- 3. ELIMINAR TABLAS BRONZE

DROP TABLE IF EXISTS smartdata_jeol.bronze.automoviles;

DROP TABLE IF EXISTS smartdata_jeol.bronze.movies;

DROP TABLE IF EXISTS smartdata_jeol.bronze.film_details;

DROP TABLE IF EXISTS smartdata_jeol.bronze.more_info;

DROP TABLE IF EXISTS smartdata_jeol.bronze.poster_path;
