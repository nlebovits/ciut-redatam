# Census Data Processing Commands

This document contains the complete command history for reorganizing and standardizing census spatial data for 2010 and 2022.

## Overview

The processing workflow converts shapefiles from EPSG:3857 (Web Mercator) to EPSG:4326 (WGS84) GeoParquet format, generates PMTiles for web visualization, and standardizes file naming conventions.

### Target Directory Structure

Each year folder (`data/2010/` and `data/2022/`) contains:
- `census-data.parquet` - Census data in long format
- `metadata.parquet` - Variable/category metadata
- `radios.parquet` - GeoParquet with census radios in EPSG:4326
- `overview.pmtiles` - PMTiles for map visualization

---

## Step-by-Step Commands

### STEP 1: Convert 2022 Shapefile to GeoParquet in EPSG:4326

```bash
# Convert shapefile to GeoParquet (preserves EPSG:3857)
uv run gpio convert geoparquet \
  "data/RADIOS_2022_V2025-1/Radios 2022 v2025-1.shp" \
  /tmp/radios_2022_3857.parquet

# Reproject from EPSG:3857 to EPSG:4326
uv run gpio convert reproject \
  /tmp/radios_2022_3857.parquet \
  data/2022/radios.parquet \
  -d EPSG:4326 \
  -s EPSG:3857 \
  --overwrite \
  -v

# Clean up temp file
rm /tmp/radios_2022_3857.parquet
```

**Result**: 66,502 features reprojected to EPSG:4326, 49 MB GeoParquet file

---

### STEP 2: Convert 2010 Shapefile to GeoParquet in EPSG:4326

```bash
# Convert shapefile to GeoParquet (preserves EPSG:3857)
uv run gpio convert geoparquet \
  "data/RADIOS_2010_V2025-1/Radios 2010 v2025-1.shp" \
  /tmp/radios_2010_3857.parquet

# Reproject from EPSG:3857 to EPSG:4326
uv run gpio convert reproject \
  /tmp/radios_2010_3857.parquet \
  data/2010/radios.parquet \
  -d EPSG:4326 \
  -s EPSG:3857 \
  --overwrite

# Clean up temp file
rm /tmp/radios_2010_3857.parquet
```

**Result**: 52,406 features reprojected to EPSG:4326, 46 MB GeoParquet file

---

### STEP 3: Generate 2022 PMTiles Overview

```bash
# Remove old PMTiles files
rm data/2022/overview.pmtiles 2>/dev/null
rm data/2022/radios-2022.pmtiles 2>/dev/null

# Generate PMTiles with gpio plugin
uv run gpio pmtiles create \
  data/2022/radios.parquet \
  data/2022/overview.pmtiles \
  -l radios \
  -v
```

**Result**: PMTiles tileset with auto-detected zoom levels 0-10, 29 MB file

---

### STEP 4: Generate 2010 PMTiles Overview

```bash
# Generate PMTiles with gpio plugin
uv run gpio pmtiles create \
  data/2010/radios.parquet \
  data/2010/overview.pmtiles \
  -l radios \
  -v
```

**Result**: PMTiles tileset with auto-detected zoom levels 0-10, 35 MB file

---

### STEP 5: Rename Files to Standard Convention

```bash
# Rename 2022 files
mv data/2022/censo-2022-largo.parquet data/2022/census-data.parquet
mv data/2022/censo-2022-metadatos.parquet data/2022/metadata.parquet

# Rename 2010 files
mv data/2010/censo_2010_largo.parquet data/2010/census-data.parquet
mv data/2010/censo_2010_metadatos.parquet data/2010/metadata.parquet
```

---

### STEP 6: Clean Up Old Files

```bash
# Remove old 2022 radios file (replaced by new one)
rm -f data/2022/radios-2022.parquet
```

---

## Verification Commands

### Check Directory Structure

```bash
tree data/2022/
tree data/2010/
```

### Inspect GeoParquet Metadata and CRS

```bash
uv run gpio inspect data/2022/radios.parquet
uv run gpio inspect data/2010/radios.parquet
```

### Verify File Sizes

```bash
ls -lh data/2022/
ls -lh data/2010/
```

---

## Consolidated One-Liners

For future batch processing, these combined command chains can process each year in one go:

### 2022 Processing

```bash
uv run gpio convert geoparquet "data/RADIOS_2022_V2025-1/Radios 2022 v2025-1.shp" /tmp/radios_2022_3857.parquet && \
uv run gpio convert reproject /tmp/radios_2022_3857.parquet data/2022/radios.parquet -d EPSG:4326 -s EPSG:3857 --overwrite -v && \
rm /tmp/radios_2022_3857.parquet && \
uv run gpio pmtiles create data/2022/radios.parquet data/2022/overview.pmtiles -l radios -v
```

### 2010 Processing

```bash
uv run gpio convert geoparquet "data/RADIOS_2010_V2025-1/Radios 2010 v2025-1.shp" /tmp/radios_2010_3857.parquet && \
uv run gpio convert reproject /tmp/radios_2010_3857.parquet data/2010/radios.parquet -d EPSG:4326 -s EPSG:3857 --overwrite && \
rm /tmp/radios_2010_3857.parquet && \
uv run gpio pmtiles create data/2010/radios.parquet data/2010/overview.pmtiles -l radios -v
```

### File Renaming (All at Once)

```bash
mv data/2022/censo-2022-largo.parquet data/2022/census-data.parquet && \
mv data/2022/censo-2022-metadatos.parquet data/2022/metadata.parquet && \
mv data/2010/censo_2010_largo.parquet data/2010/census-data.parquet && \
mv data/2010/censo_2010_metadatos.parquet data/2010/metadata.parquet && \
rm -f data/2022/radios-2022.parquet
```

---

## Final Results

### 2022 Folder (173 MB total)
- `census-data.parquet` (95 MB) - 31M rows of census variables
- `metadata.parquet` (984 KB) - Variable/category metadata
- `radios.parquet` (49 MB) - 66,502 census radios in EPSG:4326/CRS84, 10 columns
- `overview.pmtiles` (29 MB) - PMTiles tileset (zoom 0-10)

### 2010 Folder (175 MB total)
- `census-data.parquet` (94 MB) - 43M rows of census variables
- `metadata.parquet` (439 KB) - Variable/category metadata
- `radios.parquet` (46 MB) - 52,406 census radios in EPSG:4326/CRS84, 26 columns
- `overview.pmtiles` (35 MB) - PMTiles tileset (zoom 0-10)

---

## Technical Notes

### CRS Information
- **Source**: EPSG:3857 (Web Mercator) - shapefiles from INDEC
- **Target**: EPSG:4326 / OGC:CRS84 (WGS84 Geographic)
- GeoParquet 1.1+ defaults to CRS84 identifier for WGS84 coordinates
- Bounding box for Argentina: [-74.000444, -89.000000, -26.252070, -21.780773]

### Tools Used
- **geoparquet-io** (`gpio`): GeoParquet conversion and reprojection
- **gpio-pmtiles plugin**: PMTiles generation (wraps tippecanoe)
- **tippecanoe**: Vector tile generation backend
- **uv**: Python environment/tool manager

### Workflow Patterns
1. **Two-step conversion**: Shapefile → temp GeoParquet → reprojected GeoParquet
2. **Streaming PMTiles**: gpio plugin pipes GeoJSON to tippecanoe internally
3. **Explicit CRS**: Using `-s EPSG:3857` ensures correct reprojection when metadata is ambiguous

---

Generated: 2025-02-02
