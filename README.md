# Sistema de Procesamiento Lakehouse - NYC FHVHV Trip Data

Este repositorio contiene la implementación de un pipeline de datos escalable bajo una arquitectura Medallion (Bronze, Silver, Gold), diseñado para procesar y analizar grandes volúmenes de datos de viajes de vehículos de alquiler (Uber/Lyft) en Nueva York. La solución está optimizada para ejecutarse en entornos Databricks Serverless utilizando Delta Lake y Unity Catalog.

## Arquitectura del Sistema

La arquitectura sigue el patrón Medallion para garantizar la calidad y trazabilidad del dato en cada etapa del proceso:

1. **Capa Bronze (Ingesta)**: Centralización de datos crudos provenientes de Amazon S3. El proceso realiza una lectura de archivos Parquet e integra metadatos técnicos (archivo de origen, marca de tiempo de ingesta e ID de lote) para asegurar la trazabilidad completa del ciclo de vida del dato.
2. **Capa Silver (Transformación y Calidad)**: Etapa de procesamiento donde se aplican esquemas estrictos y reglas de calidad. Incluye la normalización de campos temporales, cálculo de métricas derivadas como la duración del viaje y filtrado de registros inconsistentes. Se implementa optimización física mediante Z-Ordering en la columna de tiempo de recogida para maximizar el rendimiento de las consultas analíticas.
3. **Capa Gold (Agregación de Negocio)**: Generación de tablas de indicadores clave de rendimiento (KPIs). Se calculan métricas diarias de volumen de viajes, ingresos totales (Revenue) y promedios operativos para el consumo directo por herramientas de BI.

## Estructura del Proyecto

El repositorio está organizado siguiendo estándares de ingeniería de datos para separar la lógica de negocio de los scripts de ejecución:

- **src/etl/**: Contiene el núcleo de la lógica de procesamiento encapsulado en módulos de Python (`bronze.py`, `silver.py`, `gold.py`). Esta estructura facilita la reutilización de código y la implementación de pruebas unitarias o procesos de CI/CD.
- **Raíz (01_Bronze.py, 02_Silver.py, 03_Gold.py)**: Scripts autocontenidos diseñados específicamente para ser importados y ejecutados como Notebooks en Databricks. Estos archivos invocan la lógica interna pero permiten una ejecución rápida y directa en el entorno de computación distribuida.

## Requisitos y Configuración de Clúster

Para la ejecución exitosa del pipeline en un entorno de Databricks Serverless, es imperativo configurar el acceso a los datos mediante Unity Catalog:

1. **External Location**: Se debe crear una localización externa en Databricks que apunte al bucket de S3 `s3://datalake-nyc-viajes-sebastian/`. Esto permite que el motor de ejecución gestione los permisos de lectura y escritura de forma nativa sin exponer credenciales en el código.
2. **Unity Catalog**: Los notebooks están desarrollados para interactuar con el catálogo mediante el protocolo `s3://`. Asegúrese de tener los permisos de `READ` y `WRITE` confirmados mediante la prueba de conexión en el Catalog Explorer.

## Ejecución del Pipeline

Los procesos deben ejecutarse de manera secuencial para mantener la integridad de las dependencias entre capas:

1. **Notebook 01_Bronze**: Ingesta los datos desde la zona de aterrizaje hacia la capa Delta inicial.
2. **Notebook 02_Silver**: Realiza la limpieza profunda y optimización física de los registros.
3. **Notebook 03_Gold**: Genera los agregados finales para el análisis de negocio.

---
**Desarrollado por Sebastian Posada**
