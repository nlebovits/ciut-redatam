library(tidyverse)
library(arrow)
library(duckdb)

cat("=== 1991 CENSUS DATA PROCESSING (INDEC Full Census) ===\n\n")

start_time <- Sys.time()
elapsed <- function() round(as.numeric(difftime(Sys.time(), start_time, units = "mins")), 1)

# ============================================================================
# Province lookup (from censo_1991_codigos_provincias.xlsx)
# ============================================================================
provincias <- tribble(
  ~idprov, ~nomprov,
  1, "Salta",
  2, "Buenos Aires",
  3, "Ciudad Autónoma de Buenos Aires",
  4, "San Luis",
  5, "Entre Ríos",
  6, "La Rioja",
  7, "Santiago del Estero",
  8, "Chaco",
  10, "San Juan",
  11, "Catamarca",
  12, "La Pampa",
  13, "Mendoza",
  14, "Misiones",
  16, "Formosa",
  17, "Neuquén",
  18, "Río Negro",
  19, "Santa Fe",
  20, "Tucumán",
  21, "Chubut",
  22, "Tierra del Fuego",
  23, "Corrientes",
  24, "Córdoba",
  25, "Jujuy",
  26, "Santa Cruz"
)

# ============================================================================
# Variable labels (mapping Base B names → ARG1991 label files)
# ============================================================================

# Map Base B variable names to ARG1991 label file names
var_label_map <- tribble(
  ~codigo_variable, ~label_file, ~etiqueta_variable,
  # PERSONA
  "PERSONA_PARENT", "persona_labels_parentes.csv", "Parentesco",
  "PERSONA_SEXO", "persona_labels_sexo.csv", "Sexo",
  "PERSONA_EDAD", NA_character_, "Edad",
  "PERSONA_LUGARNAC", "persona_labels_naciocodi.csv", "Lugar de nacimiento",
  "PERSONA_ASISTE", "persona_labels_asistencia.csv", "Asistencia escolar",
  "PERSONA_NIVEL", "persona_labels_nivelasist.csv", "Nivel educativo",
  "PERSONA_NIVELMAX", "persona_labels_ultimoapro.csv", "Máximo nivel aprobado",
  "PERSONA_NIVELCOM", "persona_labels_complcurso.csv", "Nivel completo",
  # HOGAR
  "HOGAR_CUARTOS", NA_character_, "Cantidad de cuartos",
  "HOGAR_DESAGUE", "hogar_labels_desaginh.csv", "Desagüe",
  "HOGAR_TENENCIA", "hogar_labels_propvivh.csv", "Tenencia de la vivienda",
  "HOGAR_BANO", "hogar_labels_inodoroh.csv", "Baño",
  "HOGAR_TIPOH", NA_character_, "Tipo de hogar",
  "HOGAR_TIPOHOG", NA_character_, "Tipo de hogar (detallado)",
  # VIVIENDA
  "VIVIENDA_LOCALIDA", NA_character_, "Localidad",
  "VIVIENDA_AREA", NA_character_, "Área",
  "VIVIENDA_USO", NA_character_, "Uso de la vivienda",
  "VIVIENDA_TIPOVIV", "vivienda_labels_tipoviv.csv", "Tipo de vivienda",
  "VIVIENDA_DESTINO", NA_character_, "Destino",
  "VIVIENDA_PISO", "vivienda_labels_pisos.csv", "Material del piso",
  "VIVIENDA_AGUA", "vivienda_labels_agua.csv", "Provisión de agua",
  "VIVIENDA_AGUAOBT", "vivienda_labels_obtagua.csv", "Obtención del agua",
  "VIVIENDA_TIPOCASA", NA_character_, "Tipo de casa",
  "VIVIENDA_VILLA", NA_character_, "Villa de emergencia"
)

# Fix corrupted Spanish characters (U+FFFD replacement char baked into source files)
fix_encoding <- function(text) {
  if (is.null(text) || length(text) == 0) return(text)

  # U+FFFD is the Unicode replacement character
  rc <- "�"

  # Specific pattern replacements
  text <- gsub(paste0("Var", rc, "n"), "Varón", text)
  text <- gsub(paste0("ca", rc, "er", rc, "a"), "cañería", text)
  text <- gsub(paste0("Asisti", rc), "Asistió", text)
  text <- gsub(paste0("asisti", rc), "asistió", text)
  text <- gsub(paste0("Jard", rc, "n"), "Jardín", text)
  text <- gsub(paste0("c", rc, "mara"), "cámara", text)
  text <- gsub(paste0("C", rc, "mara"), "Cámara", text)
  text <- gsub(paste0("s", rc, "ptic"), "séptic", text)
  text <- gsub(paste0("p", rc, "blic"), "públic", text)
  text <- gsub(paste0("habitaci", rc, "n"), "habitación", text)
  text <- gsub(paste0("Pensi", rc, "n"), "Pensión", text)
  text <- gsub(paste0("M", rc, "vil"), "Móvil", text)
  text <- gsub(paste0("Cer", rc, "mic"), "Cerámic", text)
  text <- gsub(paste0("cer", rc, "mic"), "cerámic", text)
  text <- gsub(paste0("pl", rc, "stic"), "plástic", text)
  text <- gsub(paste0("dom", rc, "stic"), "doméstic", text)
  text <- gsub(paste0("C", rc, "nyuge"), "Cónyuge", text)
  text <- gsub(paste0("c", rc, "nyuge"), "cónyuge", text)
  text <- gsub(paste0("relaci", rc, "n"), "relación", text)
  text <- gsub(paste0("pr", rc, "stamo"), "préstamo", text)
  text <- gsub(paste0("cesi", rc, "n"), "cesión", text)
  text <- gsub(rc, "ó", text)  # Default fallback: assume ó

  return(text)
}

# Load all available label files
load_labels <- function() {
  labels_dir <- "data/ARG1991"
  all_labels <- tibble()

  for (i in seq_len(nrow(var_label_map))) {
    label_file <- var_label_map$label_file[i]
    codigo_var <- var_label_map$codigo_variable[i]

    if (!is.na(label_file)) {
      file_path <- file.path(labels_dir, label_file)
      if (file.exists(file_path)) {
        tryCatch({
          lbl <- read_delim(file_path, delim = ";",
                           show_col_types = FALSE,
                           locale = locale(encoding = "UTF-8"))
          colnames(lbl) <- c("valor_categoria", "etiqueta_categoria")
          lbl$valor_categoria <- as.character(lbl$valor_categoria)
          lbl$etiqueta_categoria <- fix_encoding(lbl$etiqueta_categoria)
          lbl$codigo_variable <- codigo_var
          all_labels <- bind_rows(all_labels, lbl)
        }, error = function(e) {
          cat("  Warning: Could not load", label_file, "-", e$message, "\n")
        })
      }
    }
  }

  # Add "Sin dato" label for all variables
  sin_dato <- var_label_map %>%
    select(codigo_variable) %>%
    mutate(valor_categoria = "Sin dato", etiqueta_categoria = "Sin dato")

  all_labels <- bind_rows(all_labels, sin_dato)

  return(all_labels)
}

cat("Loading category labels from ARG1991...\n")
category_labels <- load_labels()
cat("  Loaded", nrow(category_labels), "label mappings\n\n")

# ============================================================================
# STEP 1: Initialize DuckDB and Load Data
# ============================================================================
cat("STEP 1: Initializing DuckDB and loading full census data...\n")

con <- dbConnect(duckdb())

cat("  [", elapsed(), "m] Loading PERSONAS (32.6M rows)...\n", sep = "")
dbExecute(con, "
  CREATE TABLE persona AS
  SELECT * FROM read_csv_auto(
    'data/1991/base_censo_1991_personas_b/CENSO_1991_PERSONAS_B.csv',
    delim=';', header=true, quote='\"'
  )
")
persona_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM persona")$n
cat("  [", elapsed(), "m] ✓ PERSONAS: ", format(persona_count, big.mark = ","), " rows\n", sep = "")

cat("  [", elapsed(), "m] Loading HOGARES (8.9M rows)...\n", sep = "")
dbExecute(con, "
  CREATE TABLE hogar AS
  SELECT * FROM read_csv_auto(
    'data/1991/base_censo_1991_hogares_b/CENSO_1991_HOGARES_B.csv',
    delim=';', header=true, quote='\"'
  )
")
hogar_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM hogar")$n
cat("  [", elapsed(), "m] ✓ HOGARES: ", format(hogar_count, big.mark = ","), " rows\n", sep = "")

cat("  [", elapsed(), "m] Loading VIVIENDAS (10M rows)...\n", sep = "")
dbExecute(con, "
  CREATE TABLE vivienda AS
  SELECT * FROM read_csv_auto(
    'data/1991/base_censo_1991_viviendas_b/CENSO_1991_VIVIENDAS_B.csv',
    delim=';', header=true, quote='\"'
  )
")
vivienda_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM vivienda")$n
cat("  [", elapsed(), "m] ✓ VIVIENDAS: ", format(vivienda_count, big.mark = ","), " rows\n\n", sep = "")

# ============================================================================
# STEP 2: Identify Variables to Process
# ============================================================================
cat("STEP 2: Identifying variables...\n")

persona_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'persona'")$column_name
hogar_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'hogar'")$column_name
vivienda_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'vivienda'")$column_name

geo_cols <- c("IDPROV", "IDDPTO", "IDFRAC", "IDRADIO", "SEGMENTO", "MANZANA", "IDVIV", "IDHOG", "IDPER")

persona_vars <- setdiff(persona_cols, geo_cols)
hogar_vars <- setdiff(hogar_cols, geo_cols)
vivienda_vars <- setdiff(vivienda_cols, geo_cols)

total_vars <- length(persona_vars) + length(hogar_vars) + length(vivienda_vars)
cat("  PERSONA:", length(persona_vars), "variables:", paste(persona_vars, collapse = ", "), "\n")
cat("  HOGAR:", length(hogar_vars), "variables:", paste(hogar_vars, collapse = ", "), "\n")
cat("  VIVIENDA:", length(vivienda_vars), "variables:", paste(vivienda_vars, collapse = ", "), "\n")
cat("  Total:", total_vars, "variables\n\n")

# ============================================================================
# STEP 3: Process Variables
# ============================================================================
cat("STEP 3: Processing variables...\n\n")

# NOTE: We include NULL values as "Sin dato" to ensure consistent population
# totals across all variables. The source data has known quality issues:
# - Santa Fe (IDPROV=19) has 581,112 records with NULL SEXO
# - Without "Sin dato", PERSONA_SEXO would show 2.2M for Santa Fe instead of 2.8M
# - Using "Sin dato" ensures all variables sum to the correct 32,615,528 total

temp_dir <- "temp_parquet_1991"
dir.create(temp_dir, showWarnings = FALSE)

process_variable <- function(con, var_name, entity) {
  codigo_variable <- paste0(toupper(entity), "_", toupper(var_name))
  var_file <- file.path(temp_dir, paste0(codigo_variable, ".parquet"))

  if (file.exists(var_file)) {
    return(list(status = "skipped", var_code = codigo_variable))
  }

  tryCatch({
    table_name <- switch(tolower(entity),
      "persona" = "persona",
      "hogar" = "hogar",
      "vivienda" = "vivienda"
    )

    sql <- sprintf("
      SELECT
        CONCAT(
          LPAD(CAST(IDPROV AS VARCHAR), 2, '0'),
          LPAD(CAST(IDDPTO AS VARCHAR), 3, '0'),
          LPAD(CAST(IDFRAC AS VARCHAR), 2, '0'),
          LPAD(CAST(IDRADIO AS VARCHAR), 2, '0')
        ) as id_geo,
        CAST(IDPROV AS INTEGER) as idprov,
        CAST(IDDPTO AS INTEGER) as iddpto,
        CAST(IDFRAC AS INTEGER) as idfrac,
        CAST(IDRADIO AS INTEGER) as idradio,
        COALESCE(CAST(%s AS VARCHAR), 'Sin dato') as valor_categoria,
        COUNT(*) as conteo
      FROM %s
      GROUP BY IDPROV, IDDPTO, IDFRAC, IDRADIO, %s
    ", var_name, table_name, var_name)

    result <- dbGetQuery(con, sql)

    if (nrow(result) == 0) {
      return(list(status = "empty", var_code = codigo_variable))
    }

    result$codigo_variable <- codigo_variable

    result <- result %>%
      left_join(provincias, by = "idprov") %>%
      mutate(etiqueta_categoria = NA_character_)

    write_parquet(result, var_file)

    list(status = "success", var_code = codigo_variable, n_rows = nrow(result))
  }, error = function(e) {
    list(status = "error", var_code = codigo_variable, error_message = e$message)
  })
}

current_var <- 0

cat("--- PERSONA (", length(persona_vars), " variables) ---\n", sep = "")
for (var_name in persona_vars) {
  current_var <- current_var + 1
  result <- process_variable(con, var_name, "PERSONA")

  pct <- round(100 * current_var / total_vars, 1)

  if (result$status == "success") {
    cat(sprintf("[%d/%d, %.1f%%] ✓ %s (%s rows)\n",
                current_var, total_vars, pct, result$var_code,
                format(result$n_rows, big.mark = ",")))
  } else if (result$status == "skipped") {
    cat(sprintf("[%d/%d, %.1f%%] ⊙ %s (skipped)\n", current_var, total_vars, pct, result$var_code))
  } else {
    cat(sprintf("[%d/%d, %.1f%%] ✗ %s - %s\n", current_var, total_vars, pct, result$var_code, result$error_message))
  }
}
cat("  [", elapsed(), "m] ✓ PERSONA complete\n\n", sep = "")

cat("--- HOGAR (", length(hogar_vars), " variables) ---\n", sep = "")
for (var_name in hogar_vars) {
  current_var <- current_var + 1
  result <- process_variable(con, var_name, "HOGAR")

  pct <- round(100 * current_var / total_vars, 1)

  if (result$status == "success") {
    cat(sprintf("[%d/%d, %.1f%%] ✓ %s (%s rows)\n",
                current_var, total_vars, pct, result$var_code,
                format(result$n_rows, big.mark = ",")))
  } else if (result$status == "skipped") {
    cat(sprintf("[%d/%d, %.1f%%] ⊙ %s (skipped)\n", current_var, total_vars, pct, result$var_code))
  } else {
    cat(sprintf("[%d/%d, %.1f%%] ✗ %s - %s\n", current_var, total_vars, pct, result$var_code, result$error_message))
  }
}
cat("  [", elapsed(), "m] ✓ HOGAR complete\n\n", sep = "")

cat("--- VIVIENDA (", length(vivienda_vars), " variables) ---\n", sep = "")
for (var_name in vivienda_vars) {
  current_var <- current_var + 1
  result <- process_variable(con, var_name, "VIVIENDA")

  pct <- round(100 * current_var / total_vars, 1)

  if (result$status == "success") {
    cat(sprintf("[%d/%d, %.1f%%] ✓ %s (%s rows)\n",
                current_var, total_vars, pct, result$var_code,
                format(result$n_rows, big.mark = ",")))
  } else if (result$status == "skipped") {
    cat(sprintf("[%d/%d, %.1f%%] ⊙ %s (skipped)\n", current_var, total_vars, pct, result$var_code))
  } else {
    cat(sprintf("[%d/%d, %.1f%%] ✗ %s - %s\n", current_var, total_vars, pct, result$var_code, result$error_message))
  }
}
cat("  [", elapsed(), "m] ✓ VIVIENDA complete\n\n", sep = "")

# ============================================================================
# STEP 4: Combine Parquet Files
# ============================================================================
cat("STEP 4: Combining parquet files...\n")

parquet_files <- list.files(temp_dir, pattern = "\\.parquet$", full.names = TRUE)
cat("  Found", length(parquet_files), "parquet files\n")

all_data <- map_df(parquet_files, read_parquet)
cat("  [", elapsed(), "m] Combined: ", format(nrow(all_data), big.mark = ","), " rows\n\n", sep = "")

# ============================================================================
# STEP 5: Write Final Output
# ============================================================================
cat("STEP 5: Writing final output...\n")

censo_1991_largo <- all_data %>%
  rename(
    valor_provincia = idprov,
    etiqueta_provincia = nomprov,
    valor_departamento = iddpto,
    valor_fraccion = idfrac,
    valor_radio = idradio
  ) %>%
  # Drop the NA etiqueta_categoria and join proper labels
  select(-etiqueta_categoria) %>%
  left_join(category_labels, by = c("codigo_variable", "valor_categoria")) %>%
  mutate(
    etiqueta_departamento = NA_character_,
    # Fallback: use valor_categoria as label if no label found
    etiqueta_categoria = coalesce(etiqueta_categoria, valor_categoria)
  ) %>%
  select(id_geo, valor_provincia, etiqueta_provincia,
         valor_departamento, etiqueta_departamento,
         valor_fraccion, valor_radio,
         codigo_variable, valor_categoria, etiqueta_categoria, conteo) %>%
  # Sort by id_geo for geographic clustering

  arrange(id_geo)

# Write with optimizations:
# - Sorted by id_geo (geographic clustering)
# - ~1M rows per row group
# - ZSTD compression
# - Dictionary encoding on categorical columns
# - Statistics enabled (default)
write_parquet(
  censo_1991_largo,
  "censo_1991_largo.parquet",
  compression = "zstd",
  chunk_size = 1000000L,
  use_dictionary = TRUE,
  write_statistics = TRUE
)
file_size <- round(file.info("censo_1991_largo.parquet")$size / 1024^2, 1)
cat("  [", elapsed(), "m] ✓ censo_1991_largo.parquet (", file_size, " MB)\n", sep = "")

# Build metadata with proper labels
censo_1991_metadatos <- censo_1991_largo %>%
  select(codigo_variable, valor_categoria) %>%
  distinct() %>%
  # Join category labels
  left_join(category_labels, by = c("codigo_variable", "valor_categoria")) %>%
  # Join variable labels
  left_join(var_label_map %>% select(codigo_variable, etiqueta_variable),
            by = "codigo_variable") %>%
  mutate(
    entidad = str_extract(codigo_variable, "^[^_]+"),
    nombre_variable = str_extract(codigo_variable, "[^_]+$"),
    # Fallback: use valor_categoria as label if no label found
    etiqueta_categoria = coalesce(etiqueta_categoria, valor_categoria),
    # Fallback: use nombre_variable if no etiqueta_variable
    etiqueta_variable = coalesce(etiqueta_variable, nombre_variable)
  ) %>%
  select(valor_categoria, etiqueta_categoria, codigo_variable,
         nombre_variable, etiqueta_variable, entidad)

write_parquet(
  censo_1991_metadatos,
  "censo_1991_metadatos.parquet",
  compression = "zstd",
  use_dictionary = TRUE,
  write_statistics = TRUE
)
meta_size <- round(file.info("censo_1991_metadatos.parquet")$size / 1024^2, 1)
cat("  [", elapsed(), "m] ✓ censo_1991_metadatos.parquet (", meta_size, " MB)\n\n", sep = "")

# ============================================================================
# STEP 6: Cleanup
# ============================================================================
cat("STEP 6: Cleanup...\n")
dbDisconnect(con, shutdown = TRUE)
unlink(temp_dir, recursive = TRUE)
cat("  ✓ Removed temp files and closed DuckDB\n\n")

# ============================================================================
# COMPLETE
# ============================================================================
total_time <- elapsed()

cat("=== COMPLETE ===\n")
cat("Output:\n")
cat("  - censo_1991_largo.parquet (", file_size, " MB)\n", sep = "")
cat("  - censo_1991_metadatos.parquet (", meta_size, " MB)\n", sep = "")
cat("\nTotal rows: ", format(nrow(censo_1991_largo), big.mark = ","), "\n", sep = "")
cat("Unique variables: ", n_distinct(censo_1991_largo$codigo_variable), "\n", sep = "")
cat("Unique geographies: ", n_distinct(censo_1991_largo$id_geo), "\n", sep = "")
cat("Total population (PERSONA_SEXO sum): ",
    format(sum(censo_1991_largo$conteo[censo_1991_largo$codigo_variable == "PERSONA_SEXO"]), big.mark = ","),
    "\n", sep = "")
cat("\nTotal time: ", total_time, " minutes\n", sep = "")
