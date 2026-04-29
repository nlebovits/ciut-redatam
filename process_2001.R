library(tidyverse)
library(arrow)
library(duckdb)

cat("=== 2001 CENSUS DATA PROCESSING (DuckDB) ===\n\n")

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
# Variable Label Map: Human-readable Spanish names for all variables
# ============================================================================
var_label_map <- tribble(
  ~codigo_variable, ~etiqueta_variable,
  # PERSONA
  "PERSONA_P1", "Parentesco",
  "PERSONA_P2", "Sexo",
  "PERSONA_P3", "Edad",
  "PERSONA_P4", "Sabe leer y escribir",
  "PERSONA_P5", "Usa computadora",
  "PERSONA_P7A", "Lugar de nacimiento",
  "PERSONA_P7PROV", "Provincia de nacimiento",
  "PERSONA_P7PAIS", "País de nacimiento",
  "PERSONA_P8A", "Residencia hace 5 años",
  "PERSONA_P8PROV", "Provincia hace 5 años",
  "PERSONA_P8PAIS", "País hace 5 años",
  "PERSONA_P9A", "Trabaja en otro lugar",
  "PERSONA_P9PROV", "Provincia de trabajo",
  "PERSONA_P9PAIS", "País de trabajo",
  "PERSONA_P19DIS", "Disciplina de estudio",
  "PERSONA_P20", "Estado civil",
  "PERSONA_P21", "Vive en pareja",
  "PERSONA_P22", "Tiene hijos nacidos vivos",
  "PERSONA_P295", "Calificación ocupacional",
  "PERSONA_P36", "Tamaño del establecimiento",
  "PERSONA_P37", "Aportes jubilatorios",
  "PERSONA_P38T", "Horas trabajadas",
  "PERSONA_P40", "Busca trabajo",
  "PERSONA_CP1", "Tiene documento",
  "PERSONA_CP3", "Asistencia escolar",
  "PERSONA_CP4", "Nivel educativo",
  "PERSONA_CP5", "Edad (menores de 5)",
  "PERSONA_CP7", "Categoría ocupacional",
  "PERSONA_CP9", "Descuento jubilatorio",
  "PERSONA_CP12", "Años de estudio aprobados",
  "PERSONA_CP63", "Condición de actividad",
  "PERSONA_EDADAGRU", "Grupo de edad (3 grupos)",
  "PERSONA_EDADQUI", "Grupo de edad quinquenal",
  "PERSONA_RAMA2", "Rama de actividad (CIIU)",
  "PERSONA_RAMA91", "Rama de actividad (1991)",
  # HOGAR
  "HOGAR_H1", "Cantidad de personas",
  "HOGAR_H10", "Provisión de agua",
  "HOGAR_CH6", "Tenencia de la vivienda",
  "HOGAR_CH9", "Desagüe del inodoro",
  "HOGAR_CH10", "Cocina",
  "HOGAR_CH12", "Material de paredes",
  "HOGAR_CH13", "Material de techos",
  "HOGAR_CH14", "Condición ocupacional jefe/cónyuge",
  "HOGAR_CH23", "Heladera",
  "HOGAR_CH24", "Lavarropas",
  "HOGAR_CH25", "Teléfono",
  "HOGAR_CH26", "Computadora e internet",
  "HOGAR_CH28", "Calidad de materiales (CALMAT)",
  "HOGAR_CH33", "Privación material",
  "HOGAR_CH51", "Ingreso total del hogar",
  "HOGAR_CH82", "Tipo de hogar",
  "HOGAR_CHNBI", "Necesidades básicas insatisfechas",
  "HOGAR_CHNBI1", "NBI: Vivienda inconveniente",
  "HOGAR_CHNBI2", "NBI: Condiciones sanitarias",
  "HOGAR_CHNBI3", "NBI: Hacinamiento",
  "HOGAR_CHNBI4", "NBI: Inasistencia escolar",
  "HOGAR_CHNBI5", "NBI: Capacidad de subsistencia",
  "HOGAR_HACIAGRU", "Hacinamiento (personas por cuarto)",
  # VIVIENDA
  "VIVIENDA_TIPOVIV", "Tipo de relevamiento",
  "VIVIENDA_URP", "Área urbana/rural",
  "VIVIENDA_V0T", "Motivo vivienda desocupada",
  "VIVIENDA_V4", "Tipo de vivienda",
  "VIVIENDA_CV12", "Material de paredes",
  "VIVIENDA_CV13", "Material de techos",
  "VIVIENDA_CV94", "Viviendas 1994",
  "VIVIENDA_CV94RA", "Radios 1994",
  "VIVIENDA_CV951", "Segmento 1995",
  "VIVIENDA_CV952", "Vivienda 1995",
  "VIVIENDA_CC4", "Cobertura: entrevista completa",
  "VIVIENDA_CC5", "Cobertura: rechazo",
  "VIVIENDA_CC6", "Cobertura: ausencia",
  "VIVIENDA_CC7", "Cobertura: otra causa",
  "VIVIENDA_CC9", "Cobertura: vivienda desocupada",
  "VIVIENDA_CC10", "Cobertura: uso no habitacional",
  "VIVIENDA_CC11", "Cobertura: en construcción",
  "VIVIENDA_CC12", "Cobertura: demolida",
  "VIVIENDA_LOC", "Código de localidad",
  "VIVIENDA_MUNI", "Código de municipio",
  "VIVIENDA_AGLO", "Código de aglomerado",
  "VIVIENDA_CANTPERS", "Cantidad de personas",
  "VIVIENDA_CANTHOG", "Cantidad de hogares"
)

# ============================================================================
# STEP 1: Initialize DuckDB and Load Data
# ============================================================================
cat("STEP 1: Initializing DuckDB...\n")

con <- dbConnect(duckdb())
dbExecute(con, "SET memory_limit = '48GB'")

cat("  [", elapsed(), "m] Loading CSVs into DuckDB...\n", sep = "")

dbExecute(con, "
  CREATE TABLE prov AS
  SELECT * FROM read_csv_auto('data/ARG2001/prov.csv', delim=';')
")
dbExecute(con, "
  CREATE TABLE dpto AS
  SELECT * FROM read_csv_auto('data/ARG2001/dpto.csv', delim=';')
")

# Fix encoding in province/department names (do it once here, not on 25M rows later)
prov_df <- dbGetQuery(con, "SELECT * FROM prov")
prov_df$nomprov <- fix_spanish_encoding(prov_df$nomprov)
dbExecute(con, "DROP TABLE prov")
dbWriteTable(con, "prov", prov_df)

dpto_df <- dbGetQuery(con, "SELECT * FROM dpto")
dpto_df$nomdepto <- fix_spanish_encoding(dpto_df$nomdepto)
dbExecute(con, "DROP TABLE dpto")
dbWriteTable(con, "dpto", dpto_df)
dbExecute(con, "
  CREATE TABLE frac AS
  SELECT * FROM read_csv_auto('data/ARG2001/frac.csv', delim=';')
")
dbExecute(con, "
  CREATE TABLE radio AS
  SELECT * FROM read_csv_auto('data/ARG2001/radio.csv', delim=';')
")
dbExecute(con, "
  CREATE TABLE seg AS
  SELECT * FROM read_csv_auto('data/ARG2001/seg.csv', delim=';')
")

cat("  [", elapsed(), "m] Loading PERSONA...\n", sep = "")
dbExecute(con, "
  CREATE TABLE persona AS
  SELECT * FROM read_csv_auto('data/ARG2001/persona.csv', delim=';')
")
persona_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM persona")$n
cat("  [", elapsed(), "m] ✓ PERSONA: ", format(persona_count, big.mark = ","), " rows\n", sep = "")

cat("  [", elapsed(), "m] Loading HOGAR...\n", sep = "")
dbExecute(con, "
  CREATE TABLE hogar AS
  SELECT * FROM read_csv_auto('data/ARG2001/hogar.csv', delim=';')
")
hogar_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM hogar")$n
cat("  [", elapsed(), "m] ✓ HOGAR: ", format(hogar_count, big.mark = ","), " rows\n", sep = "")

cat("  [", elapsed(), "m] Loading VIVIENDA...\n", sep = "")
dbExecute(con, "
  CREATE TABLE vivienda AS
  SELECT * FROM read_csv_auto('data/ARG2001/vivienda.csv', delim=';')
")
vivienda_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM vivienda")$n
cat("  [", elapsed(), "m] ✓ VIVIENDA: ", format(vivienda_count, big.mark = ","), " rows\n\n", sep = "")

# ============================================================================
# STEP 2: Create Geographic Lookup View
# ============================================================================
cat("STEP 2: Creating geographic lookup...\n")

dbExecute(con, "
  CREATE VIEW geo_lookup AS
  SELECT
    r.radio_ref_id,
    CONCAT(p.idprov, d.iddpto, f.idfrac, r.idradio) as id_geo,
    p.idprov, p.nomprov,
    d.iddpto, d.nomdepto as nomdpto,
    f.idfrac, r.idradio
  FROM radio r
  JOIN frac f ON r.frac_ref_id = f.frac_ref_id
  JOIN dpto d ON f.dpto_ref_id = d.dpto_ref_id
  JOIN prov p ON d.prov_ref_id = p.prov_ref_id
")

geo_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM geo_lookup")$n
cat("  [", elapsed(), "m] ✓ Geographic lookup: ", format(geo_count, big.mark = ","), " census tracts\n\n", sep = "")

# ============================================================================
# STEP 3: Parse Labels
# ============================================================================
cat("STEP 3: Parsing label files...\n")

label_files <- list.files("data/ARG2001", pattern = "_labels_.*\\.csv$", full.names = TRUE)
cat("  Found", length(label_files), "label files\n")

labels_list <- map(label_files, function(file) {
  tryCatch({
    filename <- basename(file)
    parts <- str_match(filename, "^(\\w+)_labels_(\\w+)\\.csv$")
    entity <- toupper(parts[2])
    variable <- toupper(parts[3])

    labels <- read_delim(file, delim = ";", show_col_types = FALSE,
                         locale = locale(encoding = "UTF-8"))
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

# Replace English labels with Spanish
all_labels <- all_labels %>%
  mutate(etiqueta_categoria = case_when(
    etiqueta_categoria == "NOTAPPLICABLE" ~ "No aplica",
    etiqueta_categoria == "MISSING" ~ "Sin dato",
    TRUE ~ etiqueta_categoria
  ))

cat("  [", elapsed(), "m] ✓ Parsed labels for ", n_distinct(all_labels$codigo_variable), " variables\n\n", sep = "")

# ============================================================================
# STEP 4: Get Variable Lists
# ============================================================================
cat("STEP 4: Identifying variables to process...\n")

persona_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'persona'")$column_name
hogar_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'hogar'")$column_name
vivienda_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'vivienda'")$column_name

exclude_patterns <- c("ref_id", "id_geo", "idprov", "nomprov", "iddpto", "nomdpto", "idfrac", "idradio")
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
# STEP 5: Process Variables with DuckDB
# ============================================================================
cat("STEP 5: Processing variables...\n\n")

temp_dir <- "temp_parquet_2001"
dir.create(temp_dir, showWarnings = FALSE)

process_variable_duckdb <- function(con, var_name, entity, all_labels) {
  codigo_variable <- paste0(toupper(entity), "_", toupper(var_name))
  var_file <- file.path(temp_dir, paste0(codigo_variable, ".parquet"))

  if (file.exists(var_file)) {
    return(list(status = "skipped", var_code = codigo_variable))
  }

  tryCatch({
    # Build SQL based on entity
    if (toupper(entity) == "PERSONA") {
      sql <- sprintf("
        SELECT
          g.id_geo, g.idprov, g.nomprov, g.iddpto, g.nomdpto, g.idfrac, g.idradio,
          CAST(p.%s AS VARCHAR) as valor_categoria,
          COUNT(*) as conteo
        FROM persona p
        JOIN hogar h ON p.hogar_ref_id = h.hogar_ref_id
        JOIN vivienda v ON h.vivienda_ref_id = v.vivienda_ref_id
        JOIN seg s ON v.seg_ref_id = s.seg_ref_id
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
        JOIN seg s ON v.seg_ref_id = s.seg_ref_id
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
        JOIN seg s ON v.seg_ref_id = s.seg_ref_id
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

    # Add labels
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

# Process PERSONA
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

# Process HOGAR
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

# Process VIVIENDA
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
# STEP 6: Combine and Write Final Output (all in DuckDB)
# ============================================================================
cat("STEP 6: Writing final output via DuckDB...\n")

# Free memory: drop source tables no longer needed
cat("  Dropping source tables to free memory...\n")
dbExecute(con, "DROP TABLE IF EXISTS persona")
dbExecute(con, "DROP TABLE IF EXISTS hogar")
dbExecute(con, "DROP TABLE IF EXISTS vivienda")
dbExecute(con, "DROP TABLE IF EXISTS seg")
dbExecute(con, "DROP VIEW IF EXISTS geo_lookup")

parquet_files <- list.files(temp_dir, pattern = "\\.parquet$", full.names = TRUE)
cat("  Found", length(parquet_files), "parquet files\n")

# Load all temp parquets into DuckDB
dbExecute(con, sprintf("
  CREATE TABLE all_data AS
  SELECT * FROM read_parquet('%s/*.parquet')
", temp_dir))

row_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM all_data")$n
cat("  [", elapsed(), "m] Combined: ", format(row_count, big.mark = ","), " rows\n", sep = "")

# Write censo_2001_largo.parquet directly from DuckDB
dbExecute(con, "
  COPY (
    SELECT
      id_geo,
      idprov AS valor_provincia,
      nomprov AS etiqueta_provincia,
      iddpto AS valor_departamento,
      nomdpto AS etiqueta_departamento,
      idfrac AS valor_fraccion,
      idradio AS valor_radio,
      codigo_variable,
      valor_categoria,
      COALESCE(etiqueta_categoria, valor_categoria) AS etiqueta_categoria,
      CAST(conteo AS BIGINT) AS conteo
    FROM all_data
    ORDER BY id_geo
  ) TO 'censo_2001_largo.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)
")
file_size <- round(file.info("censo_2001_largo.parquet")$size / 1024^2, 1)
cat("  [", elapsed(), "m] ✓ censo_2001_largo.parquet (", file_size, " MB)\n", sep = "")

# Register var_label_map in DuckDB for metadata join
dbWriteTable(con, "var_label_map", var_label_map, overwrite = TRUE)

# Write metadata parquet
dbExecute(con, "
  COPY (
    SELECT DISTINCT
      d.valor_categoria,
      COALESCE(d.etiqueta_categoria, d.valor_categoria) AS etiqueta_categoria,
      d.codigo_variable,
      SPLIT_PART(d.codigo_variable, '_', 2) AS nombre_variable,
      COALESCE(v.etiqueta_variable, SPLIT_PART(d.codigo_variable, '_', 2)) AS etiqueta_variable,
      SPLIT_PART(d.codigo_variable, '_', 1) AS entidad
    FROM all_data d
    LEFT JOIN var_label_map v ON d.codigo_variable = v.codigo_variable
  ) TO 'censo_2001_metadatos.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
")
meta_size <- round(file.info("censo_2001_metadatos.parquet")$size / 1024^2, 1)
cat("  [", elapsed(), "m] ✓ censo_2001_metadatos.parquet (", meta_size, " MB)\n\n", sep = "")

# ============================================================================
# STEP 8: Cleanup
# ============================================================================
cat("STEP 8: Cleanup...\n")

# Get stats before closing connection
final_stats <- dbGetQuery(con, "
  SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT codigo_variable) as unique_vars,
    COUNT(DISTINCT id_geo) as unique_geos
  FROM all_data
")

dbDisconnect(con, shutdown = TRUE)
unlink(temp_dir, recursive = TRUE)
cat("  ✓ Removed temp files and closed DuckDB\n\n")

# ============================================================================
# COMPLETE
# ============================================================================
total_time <- elapsed()

cat("=== COMPLETE ===\n")
cat("Output:\n")
cat("  - censo_2001_largo.parquet (", file_size, " MB)\n", sep = "")
cat("  - censo_2001_metadatos.parquet (", meta_size, " MB)\n", sep = "")
cat("\nTotal rows: ", format(final_stats$total_rows, big.mark = ","), "\n", sep = "")
cat("Unique variables: ", final_stats$unique_vars, "\n", sep = "")
cat("Unique geographies: ", final_stats$unique_geos, "\n", sep = "")
cat("\nTotal time: ", total_time, " minutes\n", sep = "")
