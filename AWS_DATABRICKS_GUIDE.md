# 🚀 Guía Maestra: Migración Cloud-Native (AWS + Databricks)

> **Autor:** Sebastian Posada (`sebastian_posada`)  
> **Objetivo:** Ejecución pipeline Lakehouse en Databricks Trial (AWS) con Unity Catalog y Z-Order Optimization.  
> **Costo Estimado:** < $1 USD (Usando Single Node & Auto-termination).

---

## 🏗️ 1. Arquitectura Confirmada

- **Cloud:** AWS (S3 para Storage, IAM para Seguridad).
- **Compute:** Databricks en AWS (Versión Trial Premium pero optimizada).
- **Storage Access:** Unity Catalog (External Locations) -> **Seguridad de Grado Empresarial**.
- **Formato:** Delta Lake (Bronze/Silver/Gold).
- **Optimización:** Z-ORDER BY (`trip_date`) en capa Silver.

---

## 🛠️ 2. Preparación del Código (Ya completado)

Tu archivo `src/common/config.py` ya fue actualizado para usar el protocolo `s3://` compatible con Unity Catalog.
Tus scripts ETL en `src/etl/` ya contienen la lógica de Delta Lake y Z-Order.

---

## ⚡ 3. Ejecución en Databricks (Paso a Paso)

### Paso 1: Importar Repositorio (Nivel Senior)

En lugar de subir archivos sueltos, conectaremos tu Git.

1. Ve a tu carpeta local del proyecto en Windows.
2. Abre la terminal en esa carpeta y ejecuta:

   ```powershell
   git init
   git config user.name "sebastian_posada"
   git config user.email "sposadap11@gmail.com"
   git add .
   git commit -m "Initial commit: Cloud-Native Lakehouse on AWS Databricks"
   # Crea el repo en GitHub.com con el nombre 'nyc-lakehouse-pipeline'
   git remote add origin https://github.com/sposadap11/nyc-lakehouse-pipeline.git
   git push -u origin master
   ```

### Paso 2: Ejecutar en Databricks

1. En Databricks, ve a **Workspace** -> **Users** -> Tu usuario.

### Paso 2: Ejecutar en Databricks

1. En Databricks, ve a **Workspace** -> **Users** -> Tu usuario.
2. Clic derecho -> **Import** -> selecciona **URL**.
3. **Copia y pega estas URLs exactas (una por una):**

- **Libro 01_Bronze:**
    `https://raw.githubusercontent.com/sposadap11/nyc-lakehouse-pipeline/master/Databricks_01_Bronze.py`
- **Libro 02_Silver:**
    `https://raw.githubusercontent.com/sposadap11/nyc-lakehouse-pipeline/master/Databricks_02_Silver.py`
- **Libro 03_Gold:**
    `https://raw.githubusercontent.com/sposadap11/nyc-lakehouse-pipeline/master/Databricks_03_Gold.py`

*Nota: Databricks convertirá automáticamente estos archivos .py en notebooks funcionales.*

### Paso 3: Ejecución Secuencial

1. **Ejecutar 01_Bronze:**
   - Lee de `s3://datalake-nyc-viajes-sebastian/raw/`
   - Escribe en `s3://datalake-nyc-viajes-sebastian/data/bronze` (Delta)
2. **Ejecutar 02_Silver (La magia ocurre aquí):**
   - Lee Bronze.
   - Limpia y Transforma.
   - **Ejecuta OPTIMIZE ZORDER BY (trip_date)** -> *Esto es lo que buscarán en una entrevista técnica.*
   - Escribe en `s3://.../data/silver`.
3. **Ejecutar 03_Gold:**
   - Calcula KPIs de Negocio (Revenue, Trips/Hour).
   - Escribe en `s3://.../data/gold`.

---

## 🛡️ 4. Estrategia de Defensa (Entrevista Técnica)

Cuando presentes esto, usa estos argumentos "Senior":

1. **¿Por qué Databricks en AWS y no solo EMR/Glue?**
    - *"Elegí Databricks para aprovechar **Delta Lake** nativo y **Unity Catalog**. Esto me garantiza transacciones ACID y gobernanza de datos centralizada que Glue no ofrece "out of the box" con la misma facilidad."*

2. **¿Cómo manejaste la optimización de costos?**
    - *"Configuré el clúster como **Single Node** para evitar el overhead de workers innecesarios en un dataset mediano, y activé **Auto-termination (10 mins)** para asegurar zero-waste billing. El costo total fue centavos."*

3. **¿Qué optimización de rendimiento aplicaste?**
    - *"Implementé **Z-ORDER Clustering** en la capa Silver basado en `trip_date`, ya que es el filtro más común para los analistas. Esto reduce drásticamente el I/O al hacer 'data skipping' en las consultas."*

4. **¿Por qué Unity Catalog?**
    - *"Para evitar hardcodear Access Keys (`fs.s3a.*`) en el código, lo cual es un riesgo de seguridad. Unity Catalog maneja la autenticación vía IAM Roles de forma transparente y auditable."*

---

## ✅ Checklist Final

- [x] Configuración AWS S3 + IAM
- [x] Databricks Unity Catalog Connection
- [x] Código Optimizado (Delta + Z-Order)
- [ ] Push a GitHub
- [ ] Ejecución Exitosa
