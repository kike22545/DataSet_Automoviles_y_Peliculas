
-- ==========================================
-- DATASET AUTOMOVILES Y PELICULAS
-- UNITY CATALOG
-- ==========================================

-- Crear catalogo si no existe
CREATE CATALOG IF NOT EXISTS smartdata_jeol;

-- Seleccionar catalogo
USE CATALOG smartdata_jeol;

-- Crear esquemas de arquitectura Medallon
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;
