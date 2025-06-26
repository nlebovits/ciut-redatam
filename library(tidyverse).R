library(tidyverse)
library(redatamx)
library(pivottabler)

dic<-redatam_open("/home/nissim/Downloads/Datos REDATAM_Base de viviendas particulares/Base de viviendas particulares/cpv2022.rxdb")


redatam_entities(dic)

redatam_variables(dic, "HOGAR")

tbl1 <- redatam_query(dic, "freq VIVIENDA by PROV.IDPROV by DPTO.IDPTO by RADIO.IDRADIO")

# rename the columns
colnames(tbl1) <- c("clabel", "cvalor", "rlabel", "rvalor")
    
# generate the pivot table
pt = qpvt(tbl1, rows = c("slabel","elabel"), columns = "alabel", "sum(value)" )

## TOTAL
Total de viviendas, hogares y personas por radio

## VIVIENDA

## HOGAR





# REDATAM Multi-Variable Processing Script
# Process multiple housing variables into tidy format

library(dplyr)
library(tidyr)

# Define variables to process
# Define variables to process
variables <- list(
  "H10" = "H10",     # Material predominante de los pisos
  "H11" = "H11",           # Material predominante de la cubierta exterior del techo
  "INMAT" = "INMAT",       # Calidad de los materiales
  "H13" = "H13",           # Tenencia de agua
  "H18" = "H18",           # Desagüe del inodoro
  "H19" = "H19",           # Combustible usado principalmente para cocinar
  "H15_17" = "H15_17",     # Inodoro con desacarga de agua
  "H14" = "H14",           # Procedencia del agua para beber y cocinar
  "NBI_VIV" = "NBI_VIV"    # NBI Vivienda de tipo inconveniente
 )

print("Starting REDATAM variable processing...")
print(paste("Processing", length(variables), "variables:", paste(names(variables), collapse = ", ")))

# Function to query and standardize a single variable
query_variable <- function(var_code, redatam_var, dic) {
  print(paste("Querying variable:", var_code, "(" , redatam_var, ")"))
  
  # Build the query string
  query_string <- paste("freq", redatam_var, "by PROV.IDPROV by DPTO.IDPTO by RADIO.IDRADIO")
  
  # Execute query
  result <- redatam_query(dic, query_string)
  
  # Standardize column names
  colnames(result) <- c("hvalue", "hlabel", "provvalue", "provlabel", 
                       "dptovalue", "dptolabel", "radvalue", "radlabel", "value")
  
  # Add variable identifier
  result$variable_code <- var_code
  
  # Create standardized variable_hvalue column
  result$variable_hvalue <- paste0(var_code, "_", result$hvalue)
  
  print(paste("  - Found", nrow(result), "records"))
  print(paste("  - Categories:", length(unique(result$hvalue))))
  
  return(result)
}

# Initialize containers
all_data <- list()
metadata <- data.frame()

print("\n=== QUERYING VARIABLES ===")

# Process each variable
for (var_code in names(variables)) {
  redatam_var <- variables[[var_code]]
  
  # Query the variable
  var_data <- query_variable(var_code, redatam_var, dic)
  
  # Store the data
  all_data[[var_code]] <- var_data
  
  # Extract metadata
  var_metadata <- var_data %>%
    select(variable_code, hvalue, hlabel) %>%
    distinct()
  
  metadata <- rbind(metadata, var_metadata)
  
  print(paste("✓ Completed", var_code))
  print("")
}

print("=== COMBINING DATA ===")

# Combine all data
combined_data <- do.call(rbind, all_data)
print(paste("Combined dataset has", nrow(combined_data), "total records"))

# Create geographic identifier
combined_data$geo_id <- paste(combined_data$provvalue, 
                             combined_data$dptovalue, 
                             combined_data$radvalue, 
                             sep = "-")

print("=== CREATING WIDE FORMAT ===")

# Create wide format dataset
wide_data <- combined_data %>%
  select(geo_id, provvalue, provlabel, dptovalue, dptolabel, 
         radvalue, radlabel, variable_hvalue, value) %>%
  pivot_wider(names_from = variable_hvalue, 
              values_from = value,
              values_fill = 0)

print(paste("Wide dataset dimensions:", nrow(wide_data), "rows x", ncol(wide_data), "columns"))

# Show the variable columns created
var_columns <- colnames(wide_data)[grepl("^(H10|H11|INMAT)_", colnames(wide_data))]
print(paste("Variable columns created:", length(var_columns)))
print("Column names:")
print(var_columns)

print("\n=== CREATING METADATA TABLE ===")

# Clean up metadata
metadata_final <- metadata %>%
  arrange(variable_code, hvalue) %>%
  mutate(column_name = paste0(variable_code, "_", hvalue))

print(paste("Metadata table has", nrow(metadata_final), "entries"))

print("\n=== SUMMARY ===")
print("Final datasets created:")
print("1. wide_data - Main analysis dataset with one row per census tract")
print("2. metadata_final - Lookup table for variable codes and labels")

# Display sample of wide data
print("\nSample of wide_data (first 5 rows, first 10 columns):")
print(wide_data[1:5, 1:min(10, ncol(wide_data))])

# Display sample of metadata
print("\nSample of metadata_final:")
print(head(metadata_final, 10))

print("\nProcessing complete! ✓")




### TOTALS
# Pick universal variables
pop_query <- redatam_query(dic, "freq PERSONA.P02 by PROV.IDPROV by DPTO.IDPTO by RADIO.IDRADIO")  # Sex
viv_query <- redatam_query(dic, "freq VIVIENDA.V01 by PROV.IDPROV by DPTO.IDPTO by RADIO.IDRADIO")  # Tipo vivienda  
hog_query <- redatam_query(dic, "freq HOGAR.H10 by PROV.IDPROV by DPTO.IDPTO by RADIO.IDRADIO")    # Floor material

# Standardize column names for each
colnames(pop_query) <- c("hvalue", "hlabel", "provvalue", "provlabel", "dptovalue", "dptolabel", "radvalue", "radlabel", "value")
colnames(viv_query) <- c("hvalue", "hlabel", "provvalue", "provlabel", "dptovalue", "dptolabel", "radvalue", "radlabel", "value")
colnames(hog_query) <- c("hvalue", "hlabel", "provvalue", "provlabel", "dptovalue", "dptolabel", "radvalue", "radlabel", "value")

# Aggregate to get totals by census tract
pop_totals <- pop_query %>% 
  group_by(provvalue, dptovalue, radvalue, provlabel, dptolabel, radlabel) %>%
  summarise(total_population = sum(value), .groups = 'drop')

viv_totals <- viv_query %>% 
  group_by(provvalue, dptovalue, radvalue, provlabel, dptolabel, radlabel) %>%
  summarise(total_viviendas = sum(value), .groups = 'drop')

hog_totals <- hog_query %>% 
  group_by(provvalue, dptovalue, radvalue, provlabel, dptolabel, radlabel) %>%
  summarise(total_hogares = sum(value), .groups = 'drop')


# Left join all totals to wide_data
wide_data <- wide_data %>%
  left_join(pop_totals, by = c("provvalue", "dptovalue", "radvalue", "provlabel", "dptolabel", "radlabel")) %>%
  left_join(viv_totals, by = c("provvalue", "dptovalue", "radvalue", "provlabel", "dptolabel", "radlabel")) %>%
  left_join(hog_totals, by = c("provvalue", "dptovalue", "radvalue", "provlabel", "dptolabel", "radlabel"))

library(sf)

radios <- read_sf('/home/nissim/Documents/dev/ciut-redatam/RADIOS_2022_V2025-1/Radios 2022 v2025-1.shp')

library(arrow)

write_parquet(wide_data, 'datos_censales_2022.parquet')
write_parquet(metadata_final, 'metadatos_finales_2022.parquet')