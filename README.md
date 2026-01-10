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

### Estructura de Archivos

```text
.
├── 01_Bronze.py           # Notebook de Ingesta
├── 02_Silver.py           # Notebook de Transformación
├── 03_Gold.py             # Notebook de KPIs
├── README.md              # Documentación técnica
├── src/                   # Código modular (Core)
│   ├── common/
│   │   └── config.py      # Configuraciones globales
│   └── etl/
│       ├── bronze.py      # Lógica Capa Bronze
│       ├── silver.py      # Lógica Capa Silver
│       └── gold.py        # Lógica Capa Gold
└── arquitectura_nyc.drawio # Diagrama editable
```

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

## Consideraciones CDC (Change Data Capture) y Gobierno de Datos

### Change Data Capture (CDC)

El Change Data Capture es un patrón de diseño que permite identificar y capturar los cambios incrementales en los datos de origen, en lugar de reprocesar el conjunto completo en cada ejecución. Su implementación es fundamental para sistemas de producción que manejan grandes volúmenes de información y requieren eficiencia en tiempos de procesamiento.

#### Estrategia de Incrementalidad Propuesta

Para este pipeline, se propone la siguiente estrategia de manejo de cambios:

1. **Reprocesos (Backfills)**: Se utilizaría particionamiento por fecha (`fecha_viaje`) combinado con el modo de escritura `replaceWhere` de Delta Lake. Esto permite reprocesar únicamente las particiones afectadas sin necesidad de recargar todo el histórico. Ejemplo de implementación:

   ```python
   df.write.format("delta") \
       .mode("overwrite") \
       .option("replaceWhere", "fecha_viaje >= '2025-01-15' AND fecha_viaje <= '2025-01-20'") \
       .save(ruta_silver)
   ```

2. **Llegadas Tardías (Late Arriving Data)**: Los registros que llegan después del cierre de un período se manejarían mediante una columna de `_processing_time` adicional. Al detectar datos con `pickup_datetime` anterior al último corte procesado, se ejecutaría un micro-batch de actualización usando la operación `MERGE`:

   ```python
   deltaTable.alias("target").merge(
       nuevos_datos.alias("source"),
       "target.trip_key = source.trip_key"
   ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
   ```

3. **Duplicados**: La generación de una llave única (`trip_key`) mediante hash SHA-256 de campos inmutables garantiza la idempotencia. Al utilizar `MERGE` con esta llave, los registros duplicados simplemente actualizan el existente sin generar filas adicionales.

4. **Versionado de Datos**:
   - **Append**: Para datos de solo inserción (logs, eventos) donde no se esperan modificaciones.
   - **Merge (Upsert)**: Para datos maestros o dimensiones que pueden cambiar (conductores, zonas).
   - **Soft Deletes**: En lugar de eliminar físicamente, se agrega una columna `_is_deleted` con valor booleano. Esto preserva el histórico y permite auditorías.
   - **Control de Snapshots**: Delta Lake mantiene automáticamente un historial de versiones mediante su Transaction Log. Se puede acceder a versiones anteriores con `df.read.format("delta").option("versionAsOf", 5).load(path)`.

### Gobierno de Datos y Seguridad

El gobierno de datos establece las políticas, procesos y estándares que aseguran la calidad, seguridad y disponibilidad de la información. En una arquitectura Lakehouse empresarial, esto se implementa en múltiples capas.

#### Control de Accesos por Capa

Se propone un modelo de permisos basado en el principio de menor privilegio:

| Capa | Perfil de Acceso | Justificación |
|------|------------------|---------------|
| Bronze | Solo ingenieros de datos | Datos crudos, potencialmente sensibles o con errores |
| Silver | Ingenieros de datos + Analistas senior | Datos validados pero aún granulares |
| Gold | Analistas, científicos de datos, BI | Datos agregados y listos para consumo |

En Unity Catalog, esto se implementa mediante:

```sql
GRANT SELECT ON SCHEMA gold TO `grupo_analistas`;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO `grupo_ingenieria`;
```

#### Separación de Entornos

Para garantizar estabilidad en producción, se recomienda la siguiente estructura:

- **Desarrollo (dev)**: Bucket S3 separado (`s3://datalake-nyc-dev/`), catálogo de Unity Catalog dedicado. Permite experimentación sin riesgo.
- **QA/Staging**: Réplica de la estructura de producción con un subconjunto de datos. Se ejecutan pruebas de regresión antes de promover cambios.
- **Producción (prod)**: Entorno controlado con pipelines automatizados, monitoreo activo y políticas de retención.

#### Auditoría de Accesos

Unity Catalog registra automáticamente todas las operaciones de lectura y escritura en las tablas. Adicionalmente, se pueden implementar:

- **Logs de Acceso en S3**: Habilitando Server Access Logging en el bucket para rastrear operaciones a nivel de objeto.
- **Audit Logs de Databricks**: Exportación de eventos de workspace hacia un sistema de monitoreo centralizado (CloudWatch, Datadog).

#### Trazabilidad de Transformaciones (Lineage)

Delta Lake y Unity Catalog proporcionan lineage automático que permite visualizar:

- Qué tablas alimentan a otras (dependencias upstream/downstream).
- Qué transformaciones se aplicaron en cada etapa.
- Cuándo y por quién fue modificada una tabla.

Esto se consulta directamente desde el Catalog Explorer de Databricks o mediante la API de Unity Catalog.

---
**Desarrollado por Sebastian Posada**
