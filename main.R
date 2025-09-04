library(tidyverse)
library(redatamx)
library(arrow)

# Configuration
db_path <- "/home/nissim/Downloads/spatial/Datos REDATAM_Base de viviendas particulares/Base de viviendas particulares/cpv2022.rxdb"
temp_dir <- "temp_parquet_files"

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

dir.create(temp_dir, showWarnings = FALSE)

metadata_list <- list()
start_time <- Sys.time()

cat("Processing", nrow(all_variables), "variables sequentially\n")

for (i in 1:nrow(all_variables)) {
  var_info <- all_variables[i,]
  var_start <- Sys.time()
  var_file <- file.path(temp_dir, paste0(var_info$var_code, ".parquet"))
  
  # Progress info
  pct <- round(100 * i / nrow(all_variables), 1)
  elapsed <- as.numeric(difftime(var_start, start_time, units = "mins"))
  eta <- if(i > 1) round(elapsed * (nrow(all_variables) - i) / (i - 1), 1) else NA
  mem_gb <- tryCatch(round(as.numeric(system("awk '/MemAvailable/ {print $2}' /proc/meminfo", intern = TRUE)) / 1024^2, 1), error = function(e) NA)
  
  cat(sprintf("Processing %s (%d/%d, %.1f%%, %.1fm elapsed, ETA %.1fm, %.1fGB free)\n", 
              var_info$var_code, i, nrow(all_variables), pct, elapsed, eta, mem_gb))
  
  # Skip if file already exists
  if (file.exists(var_file)) {
    cat(sprintf("  ✓ %s already exists, skipping\n", var_info$var_code))
    
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
    next
  }
  
  tryCatch({
    query_string <- paste("freq", var_info$redatam_var, "by PROV.IDPROV by DPTO.IDPTO by FRAC.IDFRAC by RADIO.IDRADIO")
    result <- redatam_query(dic, query_string)
    
    colnames(result) <- c("category_value", "category_label", "prov_value", "prov_label", 
                         "dpto_value", "dpto_label", "frac_value", "frac_label", 
                         "rad_value", "rad_label", "count")
    
    processed <- result %>%
      mutate(
        id_geo = paste0(prov_value, dpto_value, frac_value, rad_value),
        variable_code = var_info$var_code,
        category_value = as.character(category_value),
        count = as.integer(count)
      ) %>%
      select(id_geo, prov_value, prov_label, dpto_value, dpto_label, 
             frac_value, frac_label, rad_value, rad_label, 
             variable_code, category_value, category_label, count)
    
    write_parquet(processed, var_file)
    
    metadata_list[[var_info$var_code]] <- result %>%
      select(category_value, category_label) %>%
      distinct() %>%
      mutate(variable_code = var_info$var_code,
             variable_name = var_info$name,
             variable_label = var_info$label,
             entity = var_info$entity)
    
    var_time <- round(as.numeric(difftime(Sys.time(), var_start, units = "secs")), 2)
    cat(sprintf("  ✓ Completed in %.2fs, %d records\n", var_time, nrow(processed)))
    
    rm(result, processed)
    if (i %% 10 == 0) gc()
    
  }, error = function(e) {
    cat("Error processing", var_info$var_code, ":", e$message, "\n")
  })
}

processing_time <- round(as.numeric(difftime(Sys.time(), start_time, units = "mins")), 2)
cat(sprintf("Sequential processing completed in %.2f minutes\n", processing_time))

metadata_final <- do.call(rbind, metadata_list)

cat("\n=== SAVING FINAL DATASETS ===\n")

parquet_files <- list.files(temp_dir, pattern = "\\.parquet$", full.names = TRUE)
cat("Combining", length(parquet_files), "files in long format...\n")

combined_long <- map_dfr(parquet_files, function(f) {
  df <- read_parquet(f)
  df$category_value <- as.character(df$category_value)  # ensure consistent type
  df
})

cat("Columns in combined_long:\n")
print(colnames(combined_long))


# Rename columns to Spanish in final output (long format)
final_long <- combined_long %>%
  rename(
    id_geo = geo_id,
    valor_provincia = provvalue,
    etiqueta_provincia = provlabel,
    valor_departamento = dptovalue,
    etiqueta_departamento = dptolabel,
    valor_fraccion = fracvalue,
    etiqueta_fraccion = fraclabel,
    valor_radio = radvalue,
    etiqueta_radio = radlabel,
    codigo_variable = variable_code,
    valor_categoria = category_value,
    etiqueta_categoria = category_label,
    conteo = count
  )

write_parquet(final_long, "censo_2022_largo.parquet")

# Also save metadata with Spanish column names
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

unlink(temp_dir, recursive = TRUE)

cat("Output files:\n")
cat(" - censo_2022_largo.parquet (long format, Spanish columns)\n")
cat(" - censo_2022_metadatos.parquet (metadata, Spanish columns)\n")
cat("Total processing time:", processing_time, "minutes\n")
