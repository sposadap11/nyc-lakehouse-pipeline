# Proyecto Lakehouse NYC - Databricks Edition

Este proyecto implementa una solución de ingeniería de datos profesional utilizando **Databricks** y su almacenamiento interno **DBFS**, optimizado para el entorno **Serverless Community**.

## 🚀 Qué estamos haciendo

Estamos construyendo un pipeline que lee viajes de taxis/aplicaciones de NYC, los limpia, los organiza y calcula KPIs diarios (total de viajes, ingresos, etc.).

## 🏗️ La Estructura (Arquitectura Lakehouse)

1. **Capa Bronze:** Ingesta cruda con metadatos técnicos.
2. **Capa Silver:** Limpieza, estandarización y deduplicación mediante `MERGE` en tablas Delta.
3. **Capa Gold:** Agregaciones finales y KPIs de negocio.

## 🛠️ Herramientas Usadas

- **PySpark:** Motor de procesamiento.
- **Delta Lake:** Tablas con transaccionalidad ACID e idempotencia.
- **DBFS:** Sistema de archivos nativo de Databricks.

---
*Para ver cómo configurar y ejecutar paso a paso, revisa el archivo [DATABRICKS_GUIDE.md](./DATABRICKS_GUIDE.md).*
