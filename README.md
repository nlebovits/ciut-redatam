# CIUT-REDATAM

🚧 **Trabajo en Progreso** - Los datos provienen de fuentes oficiales del censo pero no han sido completamente verificados. Se agradecen contribuciones para verificación, mantenimiento y mejoras.

## Fuentes de Datos

- **Datos Censales**: [Instituto Nacional de Estadística y Censos (INDEC)](https://www.indec.gob.ar/indec/web/Institucional-Indec-BasesDeDatos-6)
- **Límites Geográficos**: [CONICET Digital](https://ri.conicet.gov.ar/handle/11336/149711)
- **Herramientas R**: [redatamx](https://ideasybits.github.io/redatamx4r/index.html)
- **Alternativas Futuras**: [open-redatam](https://github.com/litalbarkai/open-redatam)

## Descripción General

Este repositorio tiene como propósito procesar los datos del censo argentino de 2022 para convertirlos de un formato menos útil (archivos Redatam Equis - REDATAM-X) a un formato más accesible y utilizable. El procesamiento extrae todas las variables de la base de datos Redatam y las convierte a archivos Geoparquet, que actualmente se almacenan en un bucket público de AWS.

## Pasos de Procesamiento

El procesamiento es sencillo y consta de los siguientes pasos:

1. **Extracción de datos**: Se extraen los datos de la base de datos Redatam
2. **Conversión a tres archivos**:
   - **Archivo de metadatos**: Contiene los nombres de las columnas del conjunto de datos principal y los códigos que les corresponden
   - **Conjunto de datos principal**: Archivo en formato largo con las siguientes columnas (estructura detallada a continuación)
   - **Archivo de radios censales**: Contiene los radios censales de 2022 con información de descarga

### Estructura del Formato Largo

Los datos están organizados en formato largo para manejar tanto variables numéricas como categóricas de manera eficiente. Esta estructura permite:

- **Identificación geográfica**: `id_geo` identifica cada radio censal, con columnas adicionales para provincia, departamento, etc.
- **Variables en filas**: Cada variable del censo se convierte en una fila, con `codigo_variable` identificando la variable específica
- **Valores separados por tipo**: 
  - `valor_categoria` y `etiqueta_categoria` para variables categóricas
  - `conteo` para el número de observaciones en cada categoría
- **Flexibilidad**: Esta estructura permite agregar fácilmente nuevas variables sin modificar el esquema de columnas

**Ejemplo de estructura**:
```
id_geo    | valor_provincia | etiqueta_provincia | ... | codigo_variable | valor_categoria | etiqueta_categoria | conteo
020070101 | 02              | Caba               | ... | DPTO_IDPTO      | 007             | 007                | 1
020070201 | 02              | Caba               | ... | DPTO_IDPTO      | 007             | 007                | 1
```

## Acceso a los Datos

Los archivos procesados están disponibles en un bucket público de AWS. Puedes acceder a los datos de las siguientes maneras:

### Enlaces Directos

- **Metadatos**: `https://arg-fulbright-data.s3.us-east-2.amazonaws.com/censo-argentino-2022/censo-2022-metadatos.parquet`
- **Datos principales**: `https://arg-fulbright-data.s3.us-east-2.amazonaws.com/censo-argentino-2022/censo-2022-largo.parquet`
- **Radios censales**: `https://arg-fulbright-data.s3.us-east-2.amazonaws.com/censo-argentino-2022/radios-2022.parquet`

### Consultas SQL con DuckDB

Puedes consultar los datos directamente usando DuckDB. Primero configura el acceso a S3:

```sql
-- En la shell de DuckDB
INSTALL httpfs;
LOAD httpfs;
INSTALL spatial;
LOAD spatial;

-- Consultar el archivo de metadatos directamente desde el bucket público de S3
SELECT * FROM 's3://arg-fulbright-data/censo-argentino-2022/censo-2022-metadatos.parquet' LIMIT 10;

-- Obtener el esquema/estructura
DESCRIBE SELECT * FROM 's3://arg-fulbright-data/censo-argentino-2022/censo-2022-metadatos.parquet';

-- Explorar los datos principales del censo
SELECT * FROM 's3://arg-fulbright-data/censo-argentino-2022/censo-2022-largo.parquet' LIMIT 5;

-- Explorar los datos geográficos
SELECT * FROM 's3://arg-fulbright-data/censo-argentino-2022/radios-2022.parquet' LIMIT 5;

-- Ver qué hay en el archivo de metadatos (nombres de variables y descripciones)
SELECT * FROM 's3://arg-fulbright-data/censo-argentino-2022/censo-2022-metadatos.parquet';

-- Obtener nombres de columnas del archivo principal
PRAGMA table_info('s3://arg-fulbright-data/censo-argentino-2022/censo-2022-largo.parquet');

-- Contar filas en cada archivo
SELECT 'metadatos' as file, COUNT(*) as row_count FROM 's3://arg-fulbright-data/censo-argentino-2022/censo-2022-metadatos.parquet'
UNION ALL
SELECT 'largo' as file, COUNT(*) as row_count FROM 's3://arg-fulbright-data/censo-argentino-2022/censo-2022-largo.parquet'
UNION ALL  
SELECT 'radios' as file, COUNT(*) as row_count FROM 's3://arg-fulbright-data/censo-argentino-2022/radios-2022.parquet';

-- Obtener nombres únicos de variables y sus etiquetas
SELECT DISTINCT codigo_variable, etiqueta_variable 
FROM 's3://arg-fulbright-data/censo-argentino-2022/censo-2022-metadatos.parquet' 
ORDER BY codigo_variable;

-- Nombres de variables con sus conteos de categorías
SELECT codigo_variable, etiqueta_variable, COUNT(*) as num_categories
FROM 's3://arg-fulbright-data/censo-argentino-2022/censo-2022-metadatos.parquet' 
GROUP BY codigo_variable, etiqueta_variable
ORDER BY num_categories DESC;
```

### Unión de Datos Censales y Geográficos

Para combinar los datos censales con la información geográfica, puedes hacer un JOIN usando las columnas de identificación:

```sql
-- Ejemplo: Unir datos de población total con geometrías
SELECT 
    g.cod_2022,
    g.prov,
    g.depto,
    g.pob_tot_p,
    g.geometry,
    c.codigo_variable,
    c.valor_categoria,
    c.etiqueta_categoria,
    c.conteo
FROM 's3://arg-fulbright-data/censo-argentino-2022/radios-2022.parquet' g
JOIN 's3://arg-fulbright-data/censo-argentino-2022/censo-2022-largo.parquet' c
    ON g.cod_2022 = c.id_geo
WHERE c.codigo_variable = 'VIVIENDA_TOTPOBV'
LIMIT 10;
```

### Trabajo con Datos en Python

Para trabajar con los datos en Python y convertirlos a formatos geográficos:

```python
import duckdb
import pandas as pd
import geopandas as gpd

# Configurar DuckDB
con = duckdb.connect()
for cmd in [
    "INSTALL spatial",
    "LOAD spatial", 
    "INSTALL httpfs",
    "LOAD httpfs"
]:
    con.execute(cmd)

# Exportar datos combinados a archivo temporal
query = """
COPY (
    SELECT 
        g.cod_2022,
        g.prov,
        g.depto,
        g.pob_tot_p,
        g.geometry,
        c.codigo_variable,
        c.valor_categoria,
        c.etiqueta_categoria,
        c.conteo
    FROM 's3://arg-fulbright-data/censo-argentino-2022/radios-2022.parquet' g
    JOIN 's3://arg-fulbright-data/censo-argentino-2022/censo-2022-largo.parquet' c
        ON g.cod_2022 = c.id_geo
    WHERE c.codigo_variable = 'POB_TOT_P'
) TO 'temp_census_data.parquet' (FORMAT PARQUET);
"""

con.execute(query)

# Leer de vuelta en Python como GeoDataFrame
df = pd.read_parquet('temp_census_data.parquet')
df["geometry"] = gpd.GeoSeries.from_wkb(df["geometry"])
gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
```

**Nota**: Las columnas de unión son:
- `id_geo` en los datos censales (formato largo)
- `cod_2022` en los datos geográficos

## Problemas Conocidos

- **Variable PERSONA_HNVUA faltante**: Durante el procesamiento se encontró un error de ambigüedad en REDATAM donde la variable `PERSONA.HNVUA` tiene múltiples definiciones con el mismo nombre. Esto causó que la variable no se pudiera procesar correctamente y aparece como faltante en el dataset final. Aún no se ha investigado la causa raíz del problema.

- **Conversión de shapefile a geoparquet**: La conversión del archivo shapefile de los radios censales a formato geoparquet se realizó en Python en lugar de R debido a problemas de compatibilidad entre el paquete espacial `sf` de R y Linux. Idealmente, todos los scripts de procesamiento estarían en el mismo repositorio, pero se optó por Python para facilitar la conversión geográfica. El código de conversión estará disponible en el repositorio.