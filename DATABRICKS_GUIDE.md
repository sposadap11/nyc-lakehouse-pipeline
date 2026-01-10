# 🚀 Guía de Ejecución: Databricks Serverless (Free Tier)

Debido a que estás usando la versión **Serverless** de Databricks Community, el sistema tiene bloqueos de seguridad que impiden conectar S3 directamente con llaves (Access/Secret Keys) por código.

Para que tu proyecto funcione al 100% y sin errores, seguiremos el método oficial para entornos gratuitos.

---

## 1. Subir tus Datos a Databricks (Capa Bronze Raw)

1. Entra a tu espacio de [Databricks Community](https://community.cloud.databricks.com/).
2. En el menú de la izquierda, ve a **"Catalog"**.
3. Haz clic en el botón azul **"Create"** (arriba a la derecha) y selecciona **"Table"**.
4. Arrastra tu archivo de datos: `fhvhv_tripdata_2025-01.parquet`.
5. Una vez termine de subir, Databricks te mostrará la ruta, que probablemente será:
   `/FileStore/tables/fhvhv_tripdata_2025_01.parquet`.

---

## 2. Configuración en tus Notebooks

Ya no necesitas celdas con llaves `AccessKey` ni `SecretKey`. El código que he preparado lee directamente del almacenamiento interno de Databricks.

Simplemente asegúrate de que tu primera línea en el Notebook sea:

```python
RAW_DATA_PATH = "/FileStore/tables/fhvhv_tripdata_2025_01.parquet"
```

---

## 3. Estructura Lakehouse (Delta Lake)

Al ejecutar los notebooks, el sistema creará automáticamente las carpetas de las capas en:

- `dbfs:/FileStore/elite_flower_lakehouse/bronze`
- `dbfs:/FileStore/elite_flower_lakehouse/silver`
- `dbfs:/FileStore/elite_flower_lakehouse/gold`

---

## 4. Flujo de Git (Tu Portafolio)

Subir el código a GitHub demuestra que sabes orquestar capas, sin importar si los datos están en S3 o en el almacenamiento interno de Databricks.

| # | Etapa | Mensaje Sugerido |
|---|---|---|
| 1 | `chore` | `init project structure for databricks community` |
| 2 | `feat` | `bronze layer ingest with technical metadata` |
| 3 | `feat` | `silver layer transformation and merge logic` |
| 4 | `feat` | `gold layer business kpis and aggregations` |

---

## 🛡️ Guion de Defensa para la Entrevista

"Implementé una arquitectura Lakehouse completa en **Databricks**. Aunque inicialmente se contempló AWS S3, debido a las restricciones de seguridad del entorno **Serverless Community** (que bloquea configuraciones de Hadoop en caliente), decidí utilizar **DBFS** para asegurar la integridad y reproducibilidad del pipeline. Esto demuestra mi capacidad para adaptar soluciones técnicas a las limitaciones del entorno sin sacrificar la lógica de negocio ni la arquitectura de capas (Bronze/Silver/Gold)."
