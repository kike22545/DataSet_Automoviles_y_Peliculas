
# Evidencias del Proyecto
 

Este apartado presenta las evidencias de implementación del proyecto de Ingeniería de Datos desarrollado en Azure Databricks, utilizando arquitectura Medallion, Delta Lake, Unity Catalog, GitHub Actions y dashboards analíticos.

---

## 01. Almacenamiento en Azure
Implementación del almacenamiento de datos mediante Azure Data Lake Storage Gen2.

![Almacenamiento Azure](01.%20Almacenamiento%20Azure.png)

## 02. Contenedores de almacenamiento
Configuración de los contenedores para almacenar los archivos fuente y las tablas administradas.

![Contenedores](02.%20Contenedores.png)

## 03. Unity Catalog
Implementación del catálogo de datos y organización de los esquemas Bronze, Silver y Gold.

![Unity Catalog](03.%20Unity%20Catalog.png)

## 04. Capa Bronze
Ingesta de archivos fuente y almacenamiento inicial en tablas Delta.

![Bronze](04.%20Bronze.png)

## 05. Capa Silver
Transformación, limpieza y preparación de los datos.

![Silver](05.%20Silver.png)

## 06. Capa Gold
Generación de tablas analíticas para el consumo de información.

![Gold](06.%20Gold.png)

## 07. Notebooks de procesamiento
Desarrollo de notebooks PySpark para las diferentes capas de la arquitectura Medallion.

![Notebooks](07.%20Notebooks.png)

## 08. Job ETL
Configuración del Job de Databricks para ejecutar los procesos Bronze, Silver y Gold.

![Job ETL](08.%20Job%20ETL.png)

## 09. Ejecución del proceso ETL
Evidencia de la ejecución satisfactoria del flujo de procesamiento.

![Ejecución ETL](09.%20Ejecuci%C3%B3n%20ETL.png)

## 10. Grupos de seguridad
Creación de los grupos data-readers y data-analysts en Databricks.

![Grupos](10.%20Grupos.png)

## 11. Permisos en Unity Catalog
Configuración de privilegios de acceso a los datos mediante sentencias GRANT.

![Permisos](11.%20Permisos.png)

## 12. Integración continua y despliegue continuo
Configuración del workflow de GitHub Actions para automatizar el despliegue.

![CI/CD](12.%20CI_CD.png)

## 13. Despliegue en Databricks
Evidencia del despliegue de notebooks desde GitHub hacia Azure Databricks.

![Despliegue](13.%20Despliegue.png)

## 14. Dashboard analítico
Implementación de indicadores y visualizaciones mediante Databricks AI/BI Dashboards.

![Dashboard](14.%20Dashboard.png)

## 15. Dashboard publicado
Publicación del dashboard interactivo para el análisis de automóviles y películas.

![Dashboard publicado](15.%20Dashboard%20publicado.png)

---

## 16. Arquitectura del Proyecto

Diagrama de arquitectura tecnológica End-to-End del proyecto
DataSet_Automoviles_y_Peliculas, desarrollado en Microsoft Azure
y Azure Databricks.
 

![Arquitectura del Proyecto](16.Arquitectura.png)

## Tecnologías utilizadas

- Microsoft Azure
- Azure Data Lake Storage Gen2
- Azure Databricks
- Apache Spark / PySpark
- Delta Lake
- Unity Catalog
- GitHub Actions
- Databricks AI/BI Dashboards
