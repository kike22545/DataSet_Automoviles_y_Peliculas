
-- ================================================
-- PROYECTO: DATASET AUTOMOVILES Y PELICULAS
-- SEGURIDAD DE DATOS - UNITY CATALOG
-- ================================================

-- 1. ACCESO AL CATALOGO

GRANT USE CATALOG
ON CATALOG smartdata_jeol
TO `data-readers`;

GRANT USE CATALOG
ON CATALOG smartdata_jeol
TO `data-analysts`;


-- 2. PERMISOS DATA READERS
-- Acceso de lectura a la capa Gold

GRANT USE SCHEMA
ON SCHEMA smartdata_jeol.gold
TO `data-readers`;

GRANT SELECT
ON SCHEMA smartdata_jeol.gold
TO `data-readers`;


-- 3. PERMISOS DATA ANALYSTS
-- Acceso de lectura a Silver y Gold

GRANT USE SCHEMA
ON SCHEMA smartdata_jeol.silver
TO `data-analysts`;

GRANT SELECT
ON SCHEMA smartdata_jeol.silver
TO `data-analysts`;

GRANT USE SCHEMA
ON SCHEMA smartdata_jeol.gold
TO `data-analysts`;

GRANT SELECT
ON SCHEMA smartdata_jeol.gold
TO `data-analysts`;
