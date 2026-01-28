library(tidyverse)
library(redatamx)
library(arrow)
library(future)
library(furrr)
library(duckdb)

# Configuration
db_path <- "/home/nissim/Downloads/spatial/Datos REDATAM_Base de viviendas particulares/Base de viviendas particulares/cpv2022.rxdb"
temp_dir <- "temp_parquet_files"
n_workers <- 12  # Adjust based on your CPU cores



# Setup parallel processing
plan(multisession, workers = n_workers)

cat("=== CIUT-REDATAM Parallel Processing ===\n")
cat("Workers:", n_workers, "\n")

# Open database (in main process)
dic <- redatam_open(db_path)

# Get all variables
entities <- redatam_entities(dic)
all_variables <- do.call(rbind, lapply(entities$name, function(entity) {
  vars <- redatam_variables(dic, entity)
  if (nrow(vars) > 0) {
    vars$entity <- entity
    vars$redatam_var <- paste0(entity, ".", vars$name)
    vars$var_code <- paste0(entity, "_", vars$name)
    return(vars)
  }
}))

# Check for duplicate variable names
duplicates <- all_variables %>%
  group_by(name) %>%
  filter(n() > 1) %>%
  arrange(name, entity)

if (nrow(duplicates) > 0) {
  cat("\nNote: Found", nrow(duplicates), "duplicate variable names (handled by entity prefix):\n")
  print(duplicates %>% select(name, entity, var_code))
}

cat("\nTotal variables to process:", nrow(all_variables), "\n")

# Create temp directory
dir.create(temp_dir, showWarnings = FALSE)

# Check which files already exist (checkpoint/resume)
existing_files <- list.files(temp_dir, pattern = "\\.parquet$", full.names = FALSE)
existing_var_codes <- gsub("\\.parquet$", "", existing_files)
all_variables$already_exists <- all_variables$var_code %in% existing_var_codes

n_existing <- sum(all_variables$already_exists)
n_to_process <- sum(!all_variables$already_exists)

cat("Already processed:", n_existing, "variables\n")
cat("To process:", n_to_process, "variables\n\n")

# Prepare variables to process
vars_to_process <- all_variables %>% filter(!already_exists)

start_time <- Sys.time()

# Process variables in parallel
if (n_to_process > 0) {
  cat("=== PARALLEL PROCESSING ===\n")
  cat("Starting", n_workers, "parallel workers...\n\n")

  # Counter for progress tracking
  processed_count <- 0
  total_count <- nrow(vars_to_process)

  # Process function for each variable
  process_variable <- function(var_info, db_path, temp_dir, var_index, total_vars) {
    # Each worker opens its own database connection
    dic_worker <- redatamx::redatam_open(db_path)

    var_file <- file.path(temp_dir, paste0(var_info$var_code, ".parquet"))

    result <- tryCatch({
      query_string <- paste("freq", var_info$redatam_var,
                           "by PROV.IDPROV by DPTO.IDPTO by FRAC.IDFRAC by RADIO.IDRADIO")
      query_result <- redatamx::redatam_query(dic_worker, query_string)

      colnames(query_result) <- c("category_value", "category_label", "prov_value", "prov_label",
                                   "dpto_value", "dpto_label", "frac_value", "frac_label",
                                   "rad_value", "rad_label", "count")

      processed <- query_result %>%
        dplyr::mutate(
          id_geo = paste0(prov_value, dpto_value, frac_value, rad_value),
          variable_code = var_info$var_code,
          category_value = as.character(category_value),
          count = as.integer(count)
        ) %>%
        dplyr::select(id_geo, prov_value, prov_label, dpto_value, dpto_label,
                      frac_value, frac_label, rad_value, rad_label,
                      variable_code, category_value, category_label, count)

      arrow::write_parquet(processed, var_file)

      # Extract metadata
      metadata <- query_result %>%
        dplyr::select(category_value, category_label) %>%
        dplyr::distinct() %>%
        dplyr::mutate(variable_code = var_info$var_code,
                      variable_name = var_info$name,
                      variable_label = var_info$label,
                      entity = var_info$entity)

      list(
        status = "success",
        var_code = var_info$var_code,
        n_rows = nrow(processed),
        metadata = metadata
      )

    }, error = function(e) {
      list(
        status = "error",
        var_code = var_info$var_code,
        error_message = e$message
      )
    })

    return(result)
  }

  # Run parallel processing with progress
  vars_to_process$index <- 1:nrow(vars_to_process)

  results <- future_map(1:nrow(vars_to_process), function(i) {
    var_info <- vars_to_process[i, ]
    result <- process_variable(var_info, db_path, temp_dir, i, nrow(vars_to_process))

    # Progress update
    elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
    pct <- round(100 * i / nrow(vars_to_process), 1)
    eta <- if(i > 1) round(elapsed * (nrow(vars_to_process) - i) / i, 1) else NA

    if (result$status == "success") {
      cat(sprintf("[%d/%d, %.1f%%] ✓ %s (%d rows, %.1fm elapsed, ETA %.1fm)\n",
                  i, nrow(vars_to_process), pct, result$var_code, result$n_rows, elapsed, eta))
    } else {
      cat(sprintf("[%d/%d, %.1f%%] ✗ %s - ERROR: %s\n",
                  i, nrow(vars_to_process), pct, result$var_code, result$error_message))
    }

    return(result)
  }, .options = furrr_options(seed = TRUE))

  # Collect metadata from results
  metadata_list <- list()
  errors <- list()

  for (result in results) {
    if (result$status == "success") {
      metadata_list[[result$var_code]] <- result$metadata
    } else {
      errors[[result$var_code]] <- result$error_message
    }
  }

  processing_time <- round(as.numeric(difftime(Sys.time(), start_time, units = "mins")), 2)
  cat(sprintf("\n✓ Parallel processing completed in %.2f minutes\n", processing_time))

  if (length(errors) > 0) {
    cat("\n⚠ Errors encountered:\n")
    for (var_code in names(errors)) {
      cat(sprintf("  - %s: %s\n", var_code, errors[[var_code]]))
    }
  }
} else {
  cat("All variables already processed, skipping to combination step.\n")
  metadata_list <- list()
}

# Collect metadata from existing files if needed
if (n_existing > 0) {
  cat("\n=== COLLECTING METADATA FROM EXISTING FILES ===\n")
  existing_vars <- all_variables %>% filter(already_exists)

  for (i in 1:nrow(existing_vars)) {
    var_info <- existing_vars[i, ]
    var_file <- file.path(temp_dir, paste0(var_info$var_code, ".parquet"))

    if (!var_info$var_code %in% names(metadata_list)) {
      existing_data <- read_parquet(var_file)
      metadata_list[[var_info$var_code]] <- existing_data %>%
        select(category_value, category_label) %>%
        distinct() %>%
        mutate(variable_code = var_info$var_code,
               variable_name = var_info$name,
               variable_label = var_info$label,
               entity = var_info$entity)
    }
  }
}

metadata_final <- do.call(rbind, metadata_list)

cat("\n=== COMBINING FILES WITH DUCKDB ===\n")

parquet_files <- list.files(temp_dir, pattern = "\\.parquet$", full.names = TRUE)
cat("Combining", length(parquet_files), "parquet files using DuckDB...\n")

# Use DuckDB to combine files without loading into R memory
con <- dbConnect(duckdb())

# Create temporary combined file with correct column names
temp_combined <- "temp_combined.parquet"

# DuckDB query to read all parquet files and combine them
dbExecute(con, sprintf("
  COPY (
    SELECT
      id_geo,
      prov_value,
      prov_label,
      dpto_value,
      dpto_label,
      frac_value,
      frac_label,
      rad_value,
      rad_label,
      variable_code,
      category_value,
      category_label,
      count
    FROM read_parquet('%s/*.parquet')
  ) TO '%s' (FORMAT PARQUET, COMPRESSION 'ZSTD')
", temp_dir, temp_combined))

cat("✓ Files combined successfully\n")

# Now rename columns to Spanish using DuckDB
cat("Renaming columns to Spanish...\n")

dbExecute(con, sprintf("
  COPY (
    SELECT
      id_geo,
      prov_value AS valor_provincia,
      prov_label AS etiqueta_provincia,
      dpto_value AS valor_departamento,
      dpto_label AS etiqueta_departamento,
      frac_value AS valor_fraccion,
      frac_label AS etiqueta_fraccion,
      rad_value AS valor_radio,
      rad_label AS etiqueta_radio,
      variable_code AS codigo_variable,
      category_value AS valor_categoria,
      category_label AS etiqueta_categoria,
      count AS conteo
    FROM read_parquet('%s')
  ) TO 'censo_2022_largo.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')
", temp_combined))

# Clean up temp combined file
file.remove(temp_combined)

dbDisconnect(con, shutdown = TRUE)

cat("✓ Final dataset saved with Spanish column names\n")

# Save metadata with Spanish column names
cat("\n=== SAVING METADATA ===\n")

metadata_final_es <- metadata_final %>%
  rename(
    valor_categoria = category_value,
    etiqueta_categoria = category_label,
    codigo_variable = variable_code,
    nombre_variable = variable_name,
    etiqueta_variable = variable_label,
    entidad = entity
  )

write_parquet(metadata_final_es, "censo_2022_metadatos.parquet")

cat("✓ Metadata saved\n")

# Cleanup
cat("\n=== CLEANUP ===\n")
cat("Removing temporary directory...\n")
unlink(temp_dir, recursive = TRUE)

total_time <- round(as.numeric(difftime(Sys.time(), start_time, units = "mins")), 2)

cat("\n=== COMPLETE ===\n")
cat("Output files:\n")
cat(" - censo_2022_largo.parquet (long format, Spanish columns, ZSTD compressed)\n")
cat(" - censo_2022_metadatos.parquet (metadata, Spanish columns)\n")
cat(sprintf("\nTotal processing time: %.2f minutes\n", total_time))
cat(sprintf("Speedup: ~%.1fx faster than sequential\n", 90 / total_time))
