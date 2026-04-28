library(tidyverse)
library(arrow)
library(duckdb)

cat("=== 1991 CENSUS DATA PROCESSING (DuckDB) ===\n\n")

start_time <- Sys.time()
elapsed <- function() round(as.numeric(difftime(Sys.time(), start_time, units = "mins")), 1)

# ============================================================================
# Encoding Fix: Restore corrupted Spanish accents (U+FFFD → proper chars)
# ============================================================================
fix_spanish_encoding <- function(text) {
  if (is.null(text) || length(text) == 0) return(text)
  rc <- "\xef\xbf\xbd"

  text <- gsub(paste0("a", rc, "os"), "años", text)
  text <- gsub(paste0("A", rc, "os"), "Años", text)
  text <- gsub(paste0("a", rc, "o\\b"), "año", text)
  text <- gsub(paste0("ci", rc, "n"), "ción", text)
  text <- gsub(paste0("si", rc, "n"), "sión", text)
  text <- gsub(paste0("c", rc, "nyuge"), "cónyuge", text)
  text <- gsub(paste0("C", rc, "nyuge"), "Cónyuge", text)
  text <- gsub(paste0("n", rc, "cleo"), "núcleo", text)
  text <- gsub(paste0("N", rc, "cleo"), "Núcleo", text)
  text <- gsub(paste0("m", rc, "s\\b"), "más", text)
  text <- gsub(paste0("t", rc, "rmino"), "término", text)
  text <- gsub(paste0("tel", rc, "fono"), "teléfono", text)
  text <- gsub(paste0("p", rc, "blic"), "públic", text)
  text <- gsub(paste0("desag", rc, "e"), "desagüe", text)
  text <- gsub(paste0("pa", rc, "s"), "país", text)
  text <- gsub(paste0("dom", rc, "stic"), "doméstic", text)
  text <- gsub(paste0("m", rc, "rmol"), "mármol", text)
  text <- gsub(paste0("cer", rc, "mic"), "cerámic", text)
  text <- gsub(paste0("Cer", rc, "mic"), "Cerámic", text)
  text <- gsub(paste0("ca", rc, "er"), "cañer", text)
  text <- gsub(paste0("s", rc, "ptic"), "séptic", text)
  text <- gsub(paste0("s", rc, "lo"), "sólo", text)
  text <- gsub(paste0("pr", rc, "stamo"), "préstamo", text)
  text <- gsub(paste0("m", rc, "dic"), "médic", text)
  text <- gsub(paste0("c", rc, "mara"), "cámara", text)
  text <- gsub(paste0("autom", rc, "tic"), "automátic", text)
  text <- gsub(paste0("veh", rc, "cul"), "vehícul", text)
  text <- gsub(paste0("t", rc, "cnic"), "técnic", text)
  text <- gsub(paste0("Asisti", rc), "Asistió", text)
  text <- gsub(paste0("asisti", rc), "asistió", text)
  text <- gsub(paste0("aprob", rc), "aprobó", text)
  text <- gsub(paste0("b", rc, "sic"), "básic", text)
  text <- gsub(paste0("econ", rc, "mic"), "económic", text)
  text <- gsub(paste0("el", rc, "ctric"), "eléctric", text)
  text <- gsub(paste0("qu", rc, "mic"), "químic", text)
  text <- gsub(paste0("f", rc, "sic"), "físic", text)
  text <- gsub(paste0("matem", rc, "tic"), "matemátic", text)
  text <- gsub(paste0("estad", rc, "stic"), "estadístic", text)
  text <- gsub(paste0("jard", rc, "n"), "jardín", text)
  text <- gsub(paste0("com", rc, "n"), "común", text)
  text <- gsub(paste0("n", rc, "mero"), "número", text)
  text <- gsub(paste0("Mart", rc, "n"), "Martín", text)
  text <- gsub(paste0("L", rc, "pez"), "López", text)
  text <- gsub(paste0("Mor", rc, "n"), "Morón", text)
  text <- gsub(paste0("Col", rc, "n"), "Colón", text)
  text <- gsub(paste0("Jos", rc), "José", text)
  text <- gsub(paste0("Ju", rc, "rez"), "Juárez", text)
  text <- gsub(paste0("Gonz", rc, "l"), "González", text)
  text <- gsub(paste0("Rodr", rc, "guez"), "Rodríguez", text)
  text <- gsub(paste0("Fern", rc, "ndez"), "Fernández", text)
  text <- gsub(paste0("Bah", rc, "a"), "Bahía", text)
  text <- gsub(paste0("Mar", rc, "a"), "María", text)
  text <- gsub(paste0("R", rc, "o "), "Río ", text)
  text <- gsub(paste0("R", rc, "o$"), "Río", text)
  text <- gsub(paste0("Per", rc, "n"), "Perón", text)
  text <- gsub(paste0("Lan", rc, "s"), "Lanús", text)
  text <- gsub(paste0("Jun", rc, "n"), "Junín", text)
  text <- gsub(paste0("Luj", rc, "n"), "Luján", text)
  text <- gsub(paste0("Paran", rc), "Paraná", text)
  text <- gsub(paste0("G", rc, "e"), "Güe", text)
  text <- gsub(paste0("Echeverr", rc, "a"), "Echeverría", text)
  text <- gsub(paste0("Ituzaing", rc), "Ituzaingó", text)
  text <- gsub(paste0("Hip", rc, "lito"), "Hipólito", text)
  text <- gsub(paste0("Bartolom", rc), "Bartolomé", text)
  text <- gsub(paste0("Nicol", rc, "s"), "Nicolás", text)
  text <- gsub(paste0("Andr", rc, "s"), "Andrés", text)
  text <- gsub(paste0("Bol", rc, "var"), "Bolívar", text)
  text <- gsub(paste0("Su", rc, "rez"), "Suárez", text)
  text <- gsub(paste0("P", rc, "rez"), "Pérez", text)
  text <- gsub(paste0("Capit", rc, "n"), "Capitán", text)
  text <- gsub(paste0("Ca", rc, "uelas"), "Cañuelas", text)
  text <- gsub(paste0("Z", rc, "rate"), "Zárate", text)
  text <- gsub(paste0("Chascom", rc, "s"), "Chascomús", text)
  text <- gsub(paste0("Maip", rc), "Maipú", text)
  text <- gsub(paste0("Ober", rc), "Oberá", text)
  text <- gsub(paste0("Iguaz", rc), "Iguazú", text)
  text <- gsub(paste0("Bel", rc, "n"), "Belén", text)
  text <- gsub(paste0("Or", rc, "n"), "Orán", text)
  text <- gsub(paste0("Met", rc, "n"), "Metán", text)
  text <- gsub(paste0("Pueyrred", rc, "n"), "Pueyrredón", text)
  text <- gsub(paste0("Pe", rc, "aloza"), "Peñaloza", text)
  text <- gsub(paste0("Olavarr", rc, "a"), "Olavarría", text)
  text <- gsub(paste0("Gualeguaych", rc), "Gualeguaychú", text)
  text <- gsub(paste0("Guamin", rc), "Guaminí", text)
  text <- gsub(paste0("Pehuaj", rc), "Pehuajó", text)
  text <- gsub(paste0("Tapalqu", rc), "Tapalqué", text)
  text <- gsub(paste0("Salliquel", rc), "Salliquelló", text)
  text <- gsub(paste0("Andalgal", rc), "Andalgalá", text)
  text <- gsub(paste0("Guaymall", rc, "n"), "Guaymallén", text)
  text <- gsub(paste0("Tunuy", rc, "n"), "Tunuyán", text)
  text <- gsub(paste0("Taf", rc), "Tafí", text)
  text <- gsub(paste0("Jer", rc, "nimo"), "Jerónimo", text)
  text <- gsub(paste0("Ant", rc, "rtida"), "Antártida", text)
  text <- gsub(paste0("asf", rc, "ltic"), "asfáltic", text)
  text <- gsub(paste0("pl", rc, "stic"), "plástic", text)
  text <- gsub(paste0("cart", rc, "n"), "cartón", text)
  text <- gsub(paste0("hormig", rc, "n"), "hormigón", text)
  text <- gsub(paste0("Var", rc, "n"), "Varón", text)
  text <- gsub(paste0(rc, "a\\b"), "ía", text)
  text <- gsub(paste0(rc, "n\\b"), "ón", text)
  text <- gsub(paste0("^", rc, "o"), "Ño", text)
  text <- gsub(paste0(" ", rc, "o"), " Ño", text)
  text <- gsub(paste0("1", rc), "1°", text)
  text <- gsub(paste0(rc, "$"), "ó", text)
  text <- gsub(paste0(rc, " "), "ó ", text)
  text
}

# ============================================================================
# STEP 1: Initialize DuckDB and Load Data
# ============================================================================
cat("STEP 1: Initializing DuckDB...\n")

con <- dbConnect(duckdb())

cat("  [", elapsed(), "m] Loading CSVs into DuckDB...\n", sep = "")

dbExecute(con, "
  CREATE TABLE provin AS
  SELECT * FROM read_csv_auto('data/ARG1991/provin.csv', delim=';')
")
dbExecute(con, "
  CREATE TABLE depto AS
  SELECT * FROM read_csv_auto('data/ARG1991/depto.csv', delim=';')
")
dbExecute(con, "
  CREATE TABLE fraccion AS
  SELECT * FROM read_csv_auto('data/ARG1991/fraccion.csv', delim=';')
")
dbExecute(con, "
  CREATE TABLE radio AS
  SELECT * FROM read_csv_auto('data/ARG1991/radio.csv', delim=';')
")
dbExecute(con, "
  CREATE TABLE segmento AS
  SELECT * FROM read_csv_auto('data/ARG1991/segmento.csv', delim=';')
")

cat("  [", elapsed(), "m] Loading PERSONA...\n", sep = "")
dbExecute(con, "
  CREATE TABLE persona AS
  SELECT * FROM read_csv_auto('data/ARG1991/persona.csv', delim=';')
")
persona_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM persona")$n
cat("  [", elapsed(), "m] ✓ PERSONA: ", format(persona_count, big.mark = ","), " rows\n", sep = "")

cat("  [", elapsed(), "m] Loading HOGAR...\n", sep = "")
dbExecute(con, "
  CREATE TABLE hogar AS
  SELECT * FROM read_csv_auto('data/ARG1991/hogar.csv', delim=';')
")
hogar_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM hogar")$n
cat("  [", elapsed(), "m] ✓ HOGAR: ", format(hogar_count, big.mark = ","), " rows\n", sep = "")

cat("  [", elapsed(), "m] Loading VIVIENDA...\n", sep = "")
dbExecute(con, "
  CREATE TABLE vivienda AS
  SELECT * FROM read_csv_auto('data/ARG1991/vivienda.csv', delim=';')
")
vivienda_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM vivienda")$n
cat("  [", elapsed(), "m] ✓ VIVIENDA: ", format(vivienda_count, big.mark = ","), " rows\n\n", sep = "")

# ============================================================================
# STEP 2: Fix Geographic Name Encoding
# ============================================================================
cat("STEP 2: Fixing geographic name encoding...\n")

provin_df <- dbGetQuery(con, "SELECT * FROM provin")
provin_df$nombprov <- fix_spanish_encoding(provin_df$nombprov)
provin_df$nombprov <- gsub("Chubit", "Chubut", provin_df$nombprov)
provin_df$nombprov <- gsub("Santa F.*", "Santa Fe", provin_df$nombprov)
provin_df$nombprov <- gsub("Entre R.*s", "Entre Ríos", provin_df$nombprov)
provin_df$nombprov <- gsub("San Lu.*s", "San Luis", provin_df$nombprov)
provin_df$nombprov <- gsub("Neuqu.*n", "Neuquén", provin_df$nombprov)
provin_df$nombprov <- gsub("Tucum.*n", "Tucumán", provin_df$nombprov)
dbExecute(con, "DROP TABLE provin")
dbWriteTable(con, "provin", provin_df)

depto_df <- dbGetQuery(con, "SELECT * FROM depto")
depto_df$nombdepto <- fix_spanish_encoding(depto_df$nombdepto)
dbExecute(con, "DROP TABLE depto")
dbWriteTable(con, "depto", depto_df)

cat("  [", elapsed(), "m] ✓ Fixed province and department names\n\n", sep = "")

# ============================================================================
# STEP 3: Create Geographic Lookup View
# ============================================================================
cat("STEP 3: Creating geographic lookup...\n")

dbExecute(con, "
  CREATE VIEW geo_lookup AS
  SELECT
    r.radio_ref_id,
    CONCAT(p.provin, d.depto, f.fraccion, r.radio) as id_geo,
    p.provin as idprov, p.nombprov as nomprov,
    d.depto as iddpto, d.nombdepto as nomdpto,
    f.fraccion as idfrac, r.radio as idradio
  FROM radio r
  JOIN fraccion f ON r.fraccion_ref_id = f.fraccion_ref_id
  JOIN depto d ON f.depto_ref_id = d.depto_ref_id
  JOIN provin p ON d.provin_ref_id = p.provin_ref_id
")

geo_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM geo_lookup")$n
cat("  [", elapsed(), "m] ✓ Geographic lookup: ", format(geo_count, big.mark = ","), " census tracts\n\n", sep = "")

# ============================================================================
# STEP 4: Parse Labels (1991 format: varname;varname_description with header)
# ============================================================================
cat("STEP 4: Parsing label files...\n")

label_files <- list.files("data/ARG1991", pattern = "_labels_.*\\.csv$", full.names = TRUE)
cat("  Found", length(label_files), "label files\n")

labels_list <- map(label_files, function(file) {
  tryCatch({
    filename <- basename(file)
    parts <- str_match(filename, "^(\\w+)_labels_(\\w+)\\.csv$")
    entity <- toupper(parts[2])
    variable <- toupper(parts[3])

    labels <- read_delim(file, delim = ";", show_col_types = FALSE,
                         locale = locale(encoding = "UTF-8"))

    if (ncol(labels) < 2) return(NULL)
    colnames(labels) <- c("valor_categoria", "etiqueta_categoria")

    labels %>%
      mutate(
        codigo_variable = paste0(entity, "_", variable),
        valor_categoria = as.character(valor_categoria)
      )
  }, error = function(e) NULL)
})

all_labels <- bind_rows(labels_list) %>%
  filter(!is.na(valor_categoria), valor_categoria != "NA") %>%
  distinct(codigo_variable, valor_categoria, .keep_all = TRUE)

all_labels$etiqueta_categoria <- fix_spanish_encoding(all_labels$etiqueta_categoria)

cat("  [", elapsed(), "m] ✓ Parsed labels for ", n_distinct(all_labels$codigo_variable), " variables\n\n", sep = "")

# ============================================================================
# STEP 5: Get Variable Lists
# ============================================================================
cat("STEP 5: Identifying variables to process...\n")

persona_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'persona'")$column_name
hogar_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'hogar'")$column_name
vivienda_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'vivienda'")$column_name

exclude_patterns <- c("ref_id", "id_geo", "idprov", "nomprov", "iddpto", "nomdpto", "idfrac", "idradio", "idpers", "idhogar", "vivienda", "manzana")
exclude_regex <- paste(exclude_patterns, collapse = "|")

persona_vars <- persona_cols[!str_detect(tolower(persona_cols), exclude_regex)]
hogar_vars <- hogar_cols[!str_detect(tolower(hogar_cols), exclude_regex)]
vivienda_vars <- vivienda_cols[!str_detect(tolower(vivienda_cols), exclude_regex)]

total_vars <- length(persona_vars) + length(hogar_vars) + length(vivienda_vars)
cat("  Total variables:", total_vars, "\n")
cat("    PERSONA:", length(persona_vars), "\n")
cat("    HOGAR:", length(hogar_vars), "\n")
cat("    VIVIENDA:", length(vivienda_vars), "\n\n")

# ============================================================================
# STEP 6: Process Variables with DuckDB
# ============================================================================
cat("STEP 6: Processing variables...\n\n")

temp_dir <- "temp_parquet_1991"
dir.create(temp_dir, showWarnings = FALSE)

process_variable_duckdb <- function(con, var_name, entity, all_labels) {
  codigo_variable <- paste0(toupper(entity), "_", toupper(var_name))
  var_file <- file.path(temp_dir, paste0(codigo_variable, ".parquet"))

  if (file.exists(var_file)) {
    return(list(status = "skipped", var_code = codigo_variable))
  }

  tryCatch({
    if (toupper(entity) == "PERSONA") {
      sql <- sprintf("
        SELECT
          g.id_geo, g.idprov, g.nomprov, g.iddpto, g.nomdpto, g.idfrac, g.idradio,
          CAST(p.%s AS VARCHAR) as valor_categoria,
          COUNT(*) as conteo
        FROM persona p
        JOIN hogar h ON p.hogar_ref_id = h.hogar_ref_id
        JOIN vivienda v ON h.vivienda_ref_id = v.vivienda_ref_id
        JOIN segmento s ON v.segmento_ref_id = s.segmento_ref_id
        JOIN geo_lookup g ON s.radio_ref_id = g.radio_ref_id
        WHERE p.%s IS NOT NULL
        GROUP BY g.id_geo, g.idprov, g.nomprov, g.iddpto, g.nomdpto, g.idfrac, g.idradio, p.%s
      ", var_name, var_name, var_name)
    } else if (toupper(entity) == "HOGAR") {
      sql <- sprintf("
        SELECT
          g.id_geo, g.idprov, g.nomprov, g.iddpto, g.nomdpto, g.idfrac, g.idradio,
          CAST(h.%s AS VARCHAR) as valor_categoria,
          COUNT(*) as conteo
        FROM hogar h
        JOIN vivienda v ON h.vivienda_ref_id = v.vivienda_ref_id
        JOIN segmento s ON v.segmento_ref_id = s.segmento_ref_id
        JOIN geo_lookup g ON s.radio_ref_id = g.radio_ref_id
        WHERE h.%s IS NOT NULL
        GROUP BY g.id_geo, g.idprov, g.nomprov, g.iddpto, g.nomdpto, g.idfrac, g.idradio, h.%s
      ", var_name, var_name, var_name)
    } else {
      sql <- sprintf("
        SELECT
          g.id_geo, g.idprov, g.nomprov, g.iddpto, g.nomdpto, g.idfrac, g.idradio,
          CAST(v.%s AS VARCHAR) as valor_categoria,
          COUNT(*) as conteo
        FROM vivienda v
        JOIN segmento s ON v.segmento_ref_id = s.segmento_ref_id
        JOIN geo_lookup g ON s.radio_ref_id = g.radio_ref_id
        WHERE v.%s IS NOT NULL
        GROUP BY g.id_geo, g.idprov, g.nomprov, g.iddpto, g.nomdpto, g.idfrac, g.idradio, v.%s
      ", var_name, var_name, var_name)
    }

    result <- dbGetQuery(con, sql)

    if (nrow(result) == 0) {
      return(list(status = "empty", var_code = codigo_variable))
    }

    result$codigo_variable <- codigo_variable

    var_labels <- all_labels %>%
      filter(codigo_variable == !!codigo_variable) %>%
      select(valor_categoria, etiqueta_categoria)

    result <- result %>%
      left_join(var_labels, by = "valor_categoria")

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
  result <- process_variable_duckdb(con, var_name, "PERSONA", all_labels)

  el <- elapsed()
  pct <- round(100 * current_var / total_vars, 1)
  eta <- if(current_var > 1) round(el * (total_vars - current_var) / current_var, 1) else NA

  if (result$status == "success") {
    cat(sprintf("[%d/%d, %.1f%%] ✓ %s (%s rows, %.1fm, ETA %.1fm)\n",
                current_var, total_vars, pct, result$var_code,
                format(result$n_rows, big.mark = ","), el, eta))
  } else if (result$status == "skipped") {
    cat(sprintf("[%d/%d, %.1f%%] ⊙ %s (skipped)\n", current_var, total_vars, pct, result$var_code))
  } else if (result$status == "empty") {
    cat(sprintf("[%d/%d, %.1f%%] ○ %s (no data)\n", current_var, total_vars, pct, result$var_code))
  } else {
    cat(sprintf("[%d/%d, %.1f%%] ✗ %s - %s\n", current_var, total_vars, pct, result$var_code, result$error_message))
  }
}
cat("  [", elapsed(), "m] ✓ PERSONA complete\n\n", sep = "")

cat("--- HOGAR (", length(hogar_vars), " variables) ---\n", sep = "")
for (var_name in hogar_vars) {
  current_var <- current_var + 1
  result <- process_variable_duckdb(con, var_name, "HOGAR", all_labels)

  el <- elapsed()
  pct <- round(100 * current_var / total_vars, 1)
  eta <- if(current_var > 1) round(el * (total_vars - current_var) / current_var, 1) else NA

  if (result$status == "success") {
    cat(sprintf("[%d/%d, %.1f%%] ✓ %s (%s rows, %.1fm, ETA %.1fm)\n",
                current_var, total_vars, pct, result$var_code,
                format(result$n_rows, big.mark = ","), el, eta))
  } else if (result$status == "skipped") {
    cat(sprintf("[%d/%d, %.1f%%] ⊙ %s (skipped)\n", current_var, total_vars, pct, result$var_code))
  } else if (result$status == "empty") {
    cat(sprintf("[%d/%d, %.1f%%] ○ %s (no data)\n", current_var, total_vars, pct, result$var_code))
  } else {
    cat(sprintf("[%d/%d, %.1f%%] ✗ %s - %s\n", current_var, total_vars, pct, result$var_code, result$error_message))
  }
}
cat("  [", elapsed(), "m] ✓ HOGAR complete\n\n", sep = "")

cat("--- VIVIENDA (", length(vivienda_vars), " variables) ---\n", sep = "")
for (var_name in vivienda_vars) {
  current_var <- current_var + 1
  result <- process_variable_duckdb(con, var_name, "VIVIENDA", all_labels)

  el <- elapsed()
  pct <- round(100 * current_var / total_vars, 1)
  eta <- if(current_var > 1) round(el * (total_vars - current_var) / current_var, 1) else NA

  if (result$status == "success") {
    cat(sprintf("[%d/%d, %.1f%%] ✓ %s (%s rows, %.1fm, ETA %.1fm)\n",
                current_var, total_vars, pct, result$var_code,
                format(result$n_rows, big.mark = ","), el, eta))
  } else if (result$status == "skipped") {
    cat(sprintf("[%d/%d, %.1f%%] ⊙ %s (skipped)\n", current_var, total_vars, pct, result$var_code))
  } else if (result$status == "empty") {
    cat(sprintf("[%d/%d, %.1f%%] ○ %s (no data)\n", current_var, total_vars, pct, result$var_code))
  } else {
    cat(sprintf("[%d/%d, %.1f%%] ✗ %s - %s\n", current_var, total_vars, pct, result$var_code, result$error_message))
  }
}
cat("  [", elapsed(), "m] ✓ VIVIENDA complete\n\n", sep = "")

# ============================================================================
# STEP 7: Combine Parquet Files
# ============================================================================
cat("STEP 7: Combining parquet files...\n")

parquet_files <- list.files(temp_dir, pattern = "\\.parquet$", full.names = TRUE)
cat("  Found", length(parquet_files), "parquet files\n")

all_data <- map_df(parquet_files, read_parquet)
cat("  [", elapsed(), "m] Combined: ", format(nrow(all_data), big.mark = ","), " rows\n\n", sep = "")

# ============================================================================
# STEP 8: Write Final Output
# ============================================================================
cat("STEP 8: Writing final output...\n")

censo_1991_largo <- all_data %>%
  rename(
    valor_provincia = idprov,
    etiqueta_provincia = nomprov,
    valor_departamento = iddpto,
    etiqueta_departamento = nomdpto,
    valor_fraccion = idfrac,
    valor_radio = idradio
  ) %>%
  select(id_geo, valor_provincia, etiqueta_provincia,
         valor_departamento, etiqueta_departamento,
         valor_fraccion, valor_radio,
         codigo_variable, valor_categoria, etiqueta_categoria, conteo)

write_parquet(censo_1991_largo, "censo_1991_largo.parquet", compression = "zstd")
file_size <- round(file.info("censo_1991_largo.parquet")$size / 1024^2, 1)
cat("  [", elapsed(), "m] ✓ censo_1991_largo.parquet (", file_size, " MB)\n", sep = "")

variable_descriptions <- all_labels %>%
  distinct(codigo_variable) %>%
  mutate(
    entidad = str_extract(codigo_variable, "^[^_]+"),
    nombre_variable = str_extract(codigo_variable, "[^_]+$"),
    etiqueta_variable = nombre_variable
  )

censo_1991_metadatos <- censo_1991_largo %>%
  select(codigo_variable, valor_categoria, etiqueta_categoria) %>%
  distinct() %>%
  left_join(variable_descriptions, by = "codigo_variable") %>%
  select(valor_categoria, etiqueta_categoria,
         codigo_variable, nombre_variable, etiqueta_variable, entidad)

write_parquet(censo_1991_metadatos, "censo_1991_metadatos.parquet", compression = "zstd")
meta_size <- round(file.info("censo_1991_metadatos.parquet")$size / 1024^2, 1)
cat("  [", elapsed(), "m] ✓ censo_1991_metadatos.parquet (", meta_size, " MB)\n\n", sep = "")

# ============================================================================
# STEP 9: Cleanup
# ============================================================================
cat("STEP 9: Cleanup...\n")
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
cat("\nTotal time: ", total_time, " minutes\n", sep = "")
