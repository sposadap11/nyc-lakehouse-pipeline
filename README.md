# 🚕 NYC FHVHV Lakehouse Pipeline

Pipeline de datos escalable desarrollado para el análisis de viajes de vehículos de alquiler (Uber/Lyft) en NYC. Implementa una arquitectura **Lakehouse** de 3 capas sobre AWS Databricks.

## 🏗️ Arquitectura

- **Capa Bronze**: Ingesta de archivos Parquet crudos con metadatos técnicos.
- **Capa Silver**: Limpieza, validación de calidad y optimización física mediante **Z-Order**.
- **Capa Gold**: Agregación de KPIs de negocio (Ingresos totales, volumen de viajes).

## 🚀 Cómo ejecutar en Databricks

Para facilitar la entrega, he preparado scripts de importación directa que no requieren configuración manual de archivos:

1. Importa los notebooks desde las URLs de GitHub (ver guía adjunta).
2. Asegúrate de tener configurada la **External Location** en Unity Catalog para que el clúster pueda leer/escribir en S3.
3. Ejecuta los procesos en orden: `01_Bronze` -> `02_Silver` -> `03_Gold`.

## 🛠️ Tecnologías Usadas

- **PySpark**: Procesamiento distribuido.
- **Delta Lake**: Para transacciones ACID y optimización de almacenamiento.
- **Unity Catalog**: Gobernanza y seguridad cloud-native.
- **AWS S3**: Almacenamiento persistente.

---
*Desarrollado por Sebastian Posada*
