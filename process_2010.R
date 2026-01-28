library(tidyverse)
library(arrow)
library(xml2)

cat("=== 2010 CENSUS DATA PROCESSING (One Variable at a Time) ===\n\n")

start_time <- Sys.time()

# ============================================================================
# STEP 1: Load Geographic Lookups
# ============================================================================
cat("STEP 1: Loading geographic hierarchy...\n")

prov <- read_delim("data/CP2010ARG/output/PROV.csv", delim = ";",
                   locale = locale(encoding = "ISO-8859-1"), show_col_types = FALSE)
dpto <- read_delim("data/CP2010ARG/output/DPTO.csv", delim = ";",
                   locale = locale(encoding = "ISO-8859-1"), show_col_types = FALSE)
frac <- read_delim("data/CP2010ARG/output/FRAC.csv", delim = ";",
                   locale = locale(encoding = "ISO-8859-1"), show_col_types = FALSE)
radio <- read_delim("data/CP2010ARG/output/RADIO.csv", delim = ";",
                    locale = locale(encoding = "ISO-8859-1"), show_col_types = FALSE)
seg <- read_delim("data/CP2010ARG/output/SEG.csv", delim = ";",
                  locale = locale(encoding = "ISO-8859-1"), show_col_types = FALSE)

# Create complete geographic lookup
geo_lookup <- radio %>%
  left_join(frac %>% select(FRAC_REF_ID, DPTO_REF_ID, IDFRAC), by = "FRAC_REF_ID") %>%
  left_join(dpto %>% select(DPTO_REF_ID, PROV_REF_ID, IDDPTO, NOMDPTO), by = "DPTO_REF_ID") %>%
  left_join(prov %>% select(PROV_REF_ID, IDPROV, NOMPROV), by = "PROV_REF_ID") %>%
  mutate(id_geo = paste0(IDPROV, IDDPTO, IDFRAC, IDRADIO)) %>%
  select(RADIO_REF_ID, id_geo, IDPROV, NOMPROV, IDDPTO, NOMDPTO, IDFRAC, IDRADIO)

cat("✓ Geographic lookup created:", nrow(geo_lookup), "census tracts\n\n")

# ============================================================================
# STEP 2: Parse Labels
# ============================================================================
cat("STEP 2: Parsing label files...\n")

label_files <- list.files("data/CP2010ARG/output/Labels",
                          pattern = "-LABELS\\.csv$",
                          full.names = TRUE)

cat("Found", length(label_files), "label files\n")

labels_list <- map(label_files, function(file) {
  tryCatch({
    filename <- basename(file)
    parts <- str_split(str_remove(filename, "-LABELS\\.csv$"), "-")[[1]]
    entity <- parts[1]
    variable <- parts[2]

    labels <- read_delim(file, delim = ";", show_col_types = FALSE)
    colnames(labels) <- c("valor_categoria", "etiqueta_categoria")

    labels %>%
      mutate(
        codigo_variable = paste0(entity, "_", variable),
        valor_categoria = as.character(valor_categoria)
      )
  }, error = function(e) NULL)
})

all_labels <- bind_rows(labels_list)
cat("✓ Parsed labels for", n_distinct(all_labels$codigo_variable), "variables\n\n")

# ============================================================================
# STEP 3: Parse XML Metadata
# ============================================================================
cat("STEP 3: Parsing XML metadata...\n")

xml_file <- "data/CP2010ARG/output/redatam_converter_description.xml"
xml_data <- read_xml(xml_file)
entities_xml <- xml_find_all(xml_data, "//Entity")

variable_descriptions <- map_df(entities_xml, function(entity) {
  entity_name <- xml_attr(entity, "name")
  vars <- xml_find_all(entity, ".//Variable")
  if (length(vars) == 0) return(NULL)

  map_df(vars, function(var) {
    tibble(
      codigo_variable = paste0(entity_name, "_", xml_attr(var, "name")),
      etiqueta_variable = xml_attr(var, "description") %||% NA_character_,
      entidad = entity_name,
      nombre_variable = xml_attr(var, "name")
    )
  })
})

cat("✓ Parsed metadata for", nrow(variable_descriptions), "variables\n\n")

# ============================================================================
# STEP 4: Load Data Files (keep in memory for joining)
# ============================================================================
cat("STEP 4: Loading data files...\n")

cat("  Loading PERSONA (3.4GB)...\n")
persona <- read_delim("data/CP2010ARG/output/PERSONA.csv", delim = ";",
                      locale = locale(encoding = "ISO-8859-1"), show_col_types = FALSE)
cat("  ✓ Loaded", format(nrow(persona), big.mark = ","), "rows\n")

cat("  Loading HOGAR (552MB)...\n")
hogar <- read_delim("data/CP2010ARG/output/HOGAR.csv", delim = ";",
                    locale = locale(encoding = "ISO-8859-1"), show_col_types = FALSE)
cat("  ✓ Loaded", format(nrow(hogar), big.mark = ","), "rows\n")

cat("  Loading VIVIENDA (672MB)...\n")
vivienda <- read_delim("data/CP2010ARG/output/VIVIENDA.csv", delim = ";",
                       locale = locale(encoding = "ISO-8859-1"), show_col_types = FALSE)
cat("  ✓ Loaded", format(nrow(vivienda), big.mark = ","), "rows\n\n")

# ============================================================================
# STEP 5: Join to Geography (do once, keep in memory)
# ============================================================================
cat("STEP 5: Joining data to geography...\n")

# Join PERSONA through hierarchy
persona_geo <- persona %>%
  left_join(hogar %>% select(HOGAR_REF_ID, VIVIENDA_REF_ID), by = "HOGAR_REF_ID") %>%
  left_join(vivienda %>% select(VIVIENDA_REF_ID, SEG_REF_ID), by = "VIVIENDA_REF_ID") %>%
  left_join(seg %>% select(SEG_REF_ID, RADIO_REF_ID), by = "SEG_REF_ID") %>%
  left_join(geo_lookup, by = "RADIO_REF_ID")

cat("  ✓ PERSONA joined (", round(100 * sum(!is.na(persona_geo$id_geo)) / nrow(persona_geo), 2), "% success)\n", sep = "")

# Join HOGAR
hogar_geo <- hogar %>%
  left_join(vivienda %>% select(VIVIENDA_REF_ID, SEG_REF_ID), by = "VIVIENDA_REF_ID") %>%
  left_join(seg %>% select(SEG_REF_ID, RADIO_REF_ID), by = "SEG_REF_ID") %>%
  left_join(geo_lookup, by = "RADIO_REF_ID")

cat("  ✓ HOGAR joined (", round(100 * sum(!is.na(hogar_geo$id_geo)) / nrow(hogar_geo), 2), "% success)\n", sep = "")

# Join VIVIENDA
vivienda_geo <- vivienda %>%
  left_join(seg %>% select(SEG_REF_ID, RADIO_REF_ID), by = "SEG_REF_ID") %>%
  left_join(geo_lookup, by = "RADIO_REF_ID")

cat("  ✓ VIVIENDA joined (", round(100 * sum(!is.na(vivienda_geo$id_geo)) / nrow(vivienda_geo), 2), "% success)\n\n", sep = "")

# Clean up originals
rm(persona, hogar, vivienda, seg)
gc()

# ============================================================================
# STEP 6: Define Variables to Process
# ============================================================================
cat("STEP 6: Defining variables to process...\n")

# PERSONA variables (exclude REF_IDs, derived, date components, C-suffix)
persona_vars <- colnames(persona_geo)
exclude_patterns <- c("REF_ID", "id_geo", "IDPROV", "NOMPROV", "IDDPTO", "NOMDPTO", "IDFRAC", "IDRADIO",
                      "MARCA_", "CLAVE", "ACT", "CODOCUP", "CONDACT",
                      "EDADAGRU", "EDADQUI", "JEFEBAJAED", "RAMA", "CARACTER",
                      "P04D", "P04M", "P04A", "C$")
persona_vars_to_process <- persona_vars[!str_detect(persona_vars, paste(exclude_patterns, collapse = "|"))]

# HOGAR variables
hogar_vars <- colnames(hogar_geo)
hogar_vars_to_process <- hogar_vars[!str_detect(hogar_vars, paste(c(exclude_patterns, "V01$"), collapse = "|"))]

# VIVIENDA variables
vivienda_vars <- colnames(vivienda_geo)
vivienda_vars_to_process <- vivienda_vars[!str_detect(vivienda_vars, paste(exclude_patterns, collapse = "|"))]

# Create variable list
all_vars_to_process <- tibble(
  entity = c(rep("PERSONA", length(persona_vars_to_process)),
             rep("HOGAR", length(hogar_vars_to_process)),
             rep("VIVIENDA", length(vivienda_vars_to_process))),
  variable = c(persona_vars_to_process, hogar_vars_to_process, vivienda_vars_to_process),
  codigo_variable = paste0(entity, "_", variable)
)

cat("Total variables to process:", nrow(all_vars_to_process), "\n")
cat("  PERSONA:", length(persona_vars_to_process), "\n")
cat("  HOGAR:", length(hogar_vars_to_process), "\n")
cat("  VIVIENDA:", length(vivienda_vars_to_process), "\n\n")

# ============================================================================
# STEP 7: Process Each Variable (ONE AT A TIME)
# ============================================================================
cat("STEP 7: Processing variables one at a time...\n\n")

temp_dir <- "temp_parquet_2010"
dir.create(temp_dir, showWarnings = FALSE)

process_variable <- function(var_info, data_geo) {
  var_file <- file.path(temp_dir, paste0(var_info$codigo_variable, ".parquet"))

  # Skip if already processed
  if (file.exists(var_file)) {
    return(list(status = "skipped", var_code = var_info$codigo_variable))
  }

  tryCatch({
    # Select just geography columns and this variable
    data_subset <- data_geo %>%
      select(id_geo, IDPROV, NOMPROV, IDDPTO, NOMDPTO, IDFRAC, IDRADIO,
             all_of(var_info$variable)) %>%
      filter(!is.na(id_geo))

    # Convert variable to character and rename
    data_subset[[var_info$variable]] <- as.character(data_subset[[var_info$variable]])

    # Aggregate: count occurrences of each (geography, category_value) combination
    aggregated <- data_subset %>%
      rename(valor_categoria = all_of(var_info$variable)) %>%
      filter(!is.na(valor_categoria), valor_categoria != "") %>%
      group_by(id_geo, IDPROV, NOMPROV, IDDPTO, NOMDPTO, IDFRAC, IDRADIO, valor_categoria) %>%
      summarize(conteo = n(), .groups = "drop") %>%
      mutate(codigo_variable = var_info$codigo_variable)

    # Add labels
    aggregated <- aggregated %>%
      left_join(all_labels %>%
                  filter(codigo_variable == var_info$codigo_variable) %>%
                  select(valor_categoria, etiqueta_categoria),
                by = "valor_categoria")

    # Write to parquet
    write_parquet(aggregated, var_file)

    list(
      status = "success",
      var_code = var_info$codigo_variable,
      n_rows = nrow(aggregated)
    )
  }, error = function(e) {
    list(
      status = "error",
      var_code = var_info$codigo_variable,
      error_message = e$message
    )
  })
}

# Process all variables with progress tracking
for (i in 1:nrow(all_vars_to_process)) {
  var_info <- all_vars_to_process[i, ]

  # Get the right data
  if (var_info$entity == "PERSONA") {
    data_geo <- persona_geo
  } else if (var_info$entity == "HOGAR") {
    data_geo <- hogar_geo
  } else {
    data_geo <- vivienda_geo
  }

  result <- process_variable(var_info, data_geo)

  elapsed <- round(as.numeric(difftime(Sys.time(), start_time, units = "mins")), 1)
  pct <- round(100 * i / nrow(all_vars_to_process), 1)
  eta <- if(i > 1) round(elapsed * (nrow(all_vars_to_process) - i) / i, 1) else NA

  if (result$status == "success") {
    cat(sprintf("[%d/%d, %.1f%%] ✓ %s (%s rows, %.1fm elapsed, ETA %.1fm)\n",
                i, nrow(all_vars_to_process), pct, result$var_code,
                format(result$n_rows, big.mark = ","), elapsed, eta))
  } else if (result$status == "skipped") {
    cat(sprintf("[%d/%d, %.1f%%] ⊙ %s (skipped)\n",
                i, nrow(all_vars_to_process), pct, result$var_code))
  } else {
    cat(sprintf("[%d/%d, %.1f%%] ✗ %s - ERROR: %s\n",
                i, nrow(all_vars_to_process), pct, result$var_code, result$error_message))
  }
}

cat("\n✓ All variables processed\n\n")

# ============================================================================
# STEP 8: Combine All Parquet Files
# ============================================================================
cat("STEP 8: Combining all parquet files...\n")

parquet_files <- list.files(temp_dir, pattern = "\\.parquet$", full.names = TRUE)
cat("Found", length(parquet_files), "parquet files\n")

# Read and combine all
all_data <- map_df(parquet_files, read_parquet)

cat("✓ Combined to", format(nrow(all_data), big.mark = ","), "total rows\n\n")

# ============================================================================
# STEP 9: Finalize and Write Output
# ============================================================================
cat("STEP 9: Writing final parquet files...\n")

# Rename to Spanish column names
censo_2010_largo <- all_data %>%
  rename(
    valor_provincia = IDPROV,
    etiqueta_provincia = NOMPROV,
    valor_departamento = IDDPTO,
    etiqueta_departamento = NOMDPTO,
    valor_fraccion = IDFRAC,
    valor_radio = IDRADIO
  ) %>%
  select(id_geo, valor_provincia, etiqueta_provincia,
         valor_departamento, etiqueta_departamento,
         valor_fraccion, valor_radio,
         codigo_variable, valor_categoria, etiqueta_categoria, conteo)

cat("  Writing censo_2010_largo.parquet...\n")
write_parquet(censo_2010_largo, "censo_2010_largo.parquet", compression = "zstd")

file_info <- file.info("censo_2010_largo.parquet")
cat("  ✓ Written:", round(file_info$size / 1024^2, 1), "MB\n\n")

# Create metadata
cat("  Creating metadata...\n")
censo_2010_metadatos <- censo_2010_largo %>%
  select(codigo_variable, valor_categoria, etiqueta_categoria) %>%
  distinct() %>%
  left_join(variable_descriptions, by = "codigo_variable") %>%
  select(valor_categoria, etiqueta_categoria,
         codigo_variable, nombre_variable, etiqueta_variable, entidad)

cat("  Writing censo_2010_metadatos.parquet...\n")
write_parquet(censo_2010_metadatos, "censo_2010_metadatos.parquet", compression = "zstd")

metadata_info <- file.info("censo_2010_metadatos.parquet")
cat("  ✓ Written:", round(metadata_info$size / 1024^2, 1), "MB\n\n")

# ============================================================================
# STEP 10: Cleanup
# ============================================================================
cat("STEP 10: Cleaning up temporary files...\n")
unlink(temp_dir, recursive = TRUE)
cat("✓ Removed temporary directory\n\n")

# ============================================================================
# COMPLETE
# ============================================================================
total_time <- round(as.numeric(difftime(Sys.time(), start_time, units = "mins")), 2)

stats <- censo_2010_largo %>%
  summarize(
    total_rows = n(),
    n_variables = n_distinct(codigo_variable),
    n_geographies = n_distinct(id_geo)
  )

cat("=== COMPLETE ===\n")
cat("Output files:\n")
cat("  - censo_2010_largo.parquet (", round(file_info$size / 1024^2, 1), "MB)\n", sep = "")
cat("  - censo_2010_metadatos.parquet (", round(metadata_info$size / 1024^2, 1), "MB)\n", sep = "")
cat("\nTotal rows:", format(stats$total_rows, big.mark = ","), "\n")
cat("Unique variables:", stats$n_variables, "\n")
cat("Unique geographies:", stats$n_geographies, "\n")
cat("\nTotal processing time:", total_time, "minutes\n")
