library(rsyncrosim)
library(tidyverse)
library(lubridate)
library(raster)
library(rgdal)

# Setup ----
progressBar(type = "message", message = "Preparing inputs...")

## Connect to SyncroSim ----

myScenario <- scenario()

# Load Run Controls and identify iterations to run
RunControl <- datasheet(myScenario, "burnP3Plus_RunControl")
iterations <- seq(RunControl$MinimumIteration, RunControl$MaximumIteration)

# Load remaining datasheets
ResampleOption <- datasheet(myScenario, "burnP3Plus_FireResampleOption")
DeterministicIgnitionCount <- datasheet(myScenario, "burnP3Plus_DeterministicIgnitionCount") %>% unique %>% filter(Iteration %in% iterations)
DeterministicIgnitionLocation <- datasheet(myScenario, "burnP3Plus_DeterministicIgnitionLocation") %>% unique %>% filter(Iteration %in% iterations)
DeterministicBurnCondition <- datasheet(myScenario, "burnP3Plus_DeterministicBurnCondition") %>% unique %>% filter(Iteration %in% iterations)
FuelType <- datasheet(myScenario, "burnP3Plus_FuelType")
FuelTypeCrosswalk <- datasheet(myScenario, "burnP3PlusCell2Fire_FuelCodeCrosswalk", lookupsAsFactors = F)
ValidFuelCodes <- datasheet(myScenario, "burnP3PlusCell2Fire_FuelCode") %>% pull()
OutputOptions <- datasheet(myScenario, "burnP3Plus_OutputOption")
OutputOptionsSpatial <- datasheet(myScenario, "burnP3Plus_OutputOptionSpatial")

# Import relevant rasters, allowing for missing elevation
fuelsRaster <- datasheetRaster(myScenario, "burnP3Plus_LandscapeRasters", "FuelGridFileName")
elevationRaster <- tryCatch(
  datasheetRaster(myScenario, "burnP3Plus_LandscapeRasters", "ElevationGridFileName"),
  error = function(e) NULL)

## Handle empty values ----
if(nrow(FuelTypeCrosswalk) == 0) {
  updateRunLog("No fuels code crosswalk found! Using default crosswalk for Canadian Forest Service fuel codes.", type = "warning")
  FuelTypeCrosswalk <- read_csv(file.path(ssimEnvironment()$PackageDirectory, "Default Fuel Crosswalk.csv"))
  saveDatasheet(myScenario, FuelTypeCrosswalk, "burnP3PlusCell2Fire_FuelCodeCrosswalk")
}

if(nrow(OutputOptions) == 0) {
  updateRunLog("No tabular output options chosen. Defaulting to keeping all tabular outputs.", type = "info")
  OutputOptions[1,] <- rep(TRUE, length(OutputOptions[1,]))
  saveDatasheet(myScenario, OutputOptions, "burnP3Plus_OutputOption")
}

if(nrow(OutputOptionsSpatial) == 0) {
  updateRunLog("No spatial output options chosen. Defaulting to keeping all spatial outputs.", type = "info")
  OutputOptionsSpatial[1,] <- rep(TRUE, length(OutputOptionsSpatial[1,]))
  saveDatasheet(myScenario, OutputOptionsSpatial, "burnP3Plus_OutputOptionSpatial")
}

if(nrow(ResampleOption) == 0) {
  ResampleOption[1,] <- c(0,0)
  saveDatasheet(myScenario, ResampleOption, "burnP3Plus_FireResampleOption")
}

## Extract relevant parameters ----

# Burn maps must be kept to generate summarized maps later, this boolean summarizes
# whether or not burn maps are needed
saveBurnMaps <- any(OutputOptionsSpatial$BurnMap, OutputOptionsSpatial$BurnProbability, OutputOptionsSpatial$BurnCount)

minimumFireSize <- ResampleOption$MinimumFireSize

# Combine fuel type definitions with codes if provided
if(nrow(FuelTypeCrosswalk) > 0) {
  FuelType <- FuelType %>%
    left_join(FuelTypeCrosswalk, by = c("Name" = "FuelType"))
} else
  FuelType <- FuelType %>%
    mutate(
      Code = Name,
      PercentConifer = NA_integer_,
      PercentDeadFir = NA_integer_,
      GrassFuelLoading = NA_real_,
      GrassCuring = NA_integer_)

## Error check fuels ----

# Ensure all fuel types are assigned to a fuel code
if(any(is.na(FuelType$Code)))
  stop("Could not find a valid Cell2Fire Fuel Code for one or more Fuel Types. Please add Cell2Fire Fuel Code Crosswalk records for the following Fuel Types in the project scope: ", 
       FuelType %>% filter(is.na(Code)) %>% pull(Name) %>% str_c(collapse = "; "))

# Ensure all fuel codes are valid
# - This should only occur if the Fuel Code Crosswalk is empty and Fuel Type names are being used as codes
if(any(!FuelType$Code %in% ValidFuelCodes))
  stop("Invalid fuel codes found in the Fuel Type definitions. Please consider setting an exlicit Fuel Code Crosswalk for Cell2Fire in the project scope.")

# Ensure that there are no fuels present in the grid that are not tied to a valid code
fuelIdsPresent <- fuelsRaster %>% unique()
if(any(!fuelIdsPresent %in% FuelType$ID))
  stop("Found one or more values in the Fuels Map that are not assigned to a known Fuel Type. Please add definitions for the following Fuel IDs: ", 
       setdiff(fuelIdsPresent, FuelType$ID) %>% str_c(collapse = " "))

## Setup files and folders ----

# Copy Cell2Fire executable
setwd(ssimEnvironment()$TempDirectory)
file.copy(file.path(ssimEnvironment()$PackageDirectory, "Cell2Fire.exe"), ssimEnvironment()$TempDirectory, overwrite = T)

# Create temp folder, ensure it is empty
tempDir <- "cell2fire-inputs"
unlink(tempDir, recursive = T, force = T)
dir.create(tempDir, showWarnings = F)

weatherFolder <- file.path(tempDir, "Weathers")
unlink(weatherFolder, recursive = T, force = T)
dir.create(weatherFolder, showWarnings = F)

# Set names for model input files
mapMetadataFile <- file.path(tempDir, "Forest.asc")
spatialDataFile <- file.path(tempDir, "Data.csv")
ignitionFile    <- file.path(tempDir, "Ignitions.csv")

# Create folders for various outputs
gridOutputFolder <- "cell2fire-outputs"
accumulatorOutputFolder <- "cell2fire-accumulator"
unlink(gridOutputFolder, recursive = T, force = T)
unlink(accumulatorOutputFolder, recursive = T, force = T)
dir.create(gridOutputFolder, showWarnings = F)
dir.create(accumulatorOutputFolder, showWarnings = F)

# Create placeholder rasters for potential outputs
burnAccumulator <- raster(fuelsRaster)
dataType(burnAccumulator) <- "INT4S"
NAvalue(burnAccumulator) <- -9999

## Function Definitions ----

### Convenience and conversion functions ----

# Define a function to facilitate recoding values using a lookup table
lookup <- function(x, old, new) dplyr::recode(x, !!!set_names(new, old))

# Get burn area from output csv
getBurnArea <- function(inputFile) {
  inputFile %>%
    read_csv(col_names = F, col_types = cols(.default = col_integer())) %>%
    as.matrix %>%
    sum %>%
    return
}

# Function to call Pandora on the (global) parameter file
runCell2Fire <- function() {
  # Format folder paths
  inputInstanceFolder <- tempDir %>%
    str_c("\\\\")
   outputFolder <- gridOutputFolder %>%
    str_c("\\\\")
  
  shell(str_c("Cell2Fire.exe",
              " --input-instance-folder ", inputInstanceFolder,
              " --output-folder ", outputFolder,
              " --nsims ", nrow(DeterministicIgnitionLocation),
              " --nweathers ", nrow(DeterministicIgnitionLocation),
              " --ignitions --final-grid --Fire-Period-Length 1.0 --weather sequential"))
}

### File generation functions ----

# Function to convert daily weather data for every day of burning to format
# expected by Pandora and save to file
generateWeatherFile <- function(weatherData, uniqueFireIndex) {
  weatherData %>%
    # To convert daily weather to hourly, we need to repeat each row for every
    # hour burned that day.
    slice(map2(.$BurnDay, .$HoursBurning, rep) %>% unlist) %>%
    # Next we add in columns of mock date and time since this is requried by Pandora
    mutate(
      date = as.integer((row_number() - 1) / 24) + ymd(20000101),
      date = str_c(month(date), "/", day(date), "/", year(date)),
      time = (row_number() - 1) %% 24,
      datetime = str_c(date, " ", time, ":00")) %>%
    # Finally we rename and reorder columns and write to file
    dplyr::transmute(
      Scenario = "Run",
      datetime = datetime,
      APCP = Precipitation,
      TMP = Temperature,
      RH = RelativeHumidity,
      WS = WindSpeed,
      WD = WindDirection,
      FFMC = FineFuelMoistureCode,
      DMC = DuffMoistureCode,
      DC = DroughtCode,
      ISI = InitialSpreadIndex,
      BUI = BuildupIndex,
      FWI = FireWeatherIndex) %>%
    write_csv(file.path(weatherFolder, str_c("Weather", uniqueFireIndex, ".csv")), escape = "none")
  invisible()
}

# Function to split deterministic burn conditions into separate weather files by iteration and fire id
# - An empty weather file is also required due to sloppy argument parsing in C2F
generateWeatherFiles <- function(DeterministicBurnCondition){
  DeterministicBurnCondition %>%
    group_by(Iteration, FireID) %>%
    nest() %>%
    ungroup() %>%
    arrange(Iteration, FireID) %>%
    transmute(weatherData = data, uniqueFireIndex = row_number()) %>%
    pmap(generateWeatherFile)
  
  tibble(
      Scenario = NA,
      datetime = NA,
      APCP = NA,
      TMP = NA,
      RH = NA,
      WS = NA,
      WD = NA,
      FFMC = NA,
      DMC = NA,
      DC = NA,
      ISI = NA,
      BUI = NA,
      FWI = NA) %>%
  write_csv(file.path(tempDir, "Weather.csv"), na = "")
  invisible()
}

# Function to create an ignition location file
generateIgnitionFile <- function(CellIDs){
  tibble(Year = seq_along(CellIDs), Ncell = CellIDs) %>%
    write_csv(ignitionFile)
}

# Prepare Shared Inputs ----

# Raster metadata
# - cell2fire uses the Forest.asc exclusively to read metadata
# - to save disk space we only write metatdata to this file
mapMetadata <- str_c("ncols ",        ncol(fuelsRaster),    "\n",
                     "nrows ",        nrow(fuelsRaster),    "\n",
                     "xllcorner ",    xmin(fuelsRaster),    "\n",
                     "yllcorner ",    ymin(fuelsRaster),    "\n",
                     "cellsize ",     res(fuelsRaster)[1],  "\n",
                     "NODATA_value ", NAvalue(fuelsRaster), "\n")

write_file(mapMetadata, mapMetadataFile)

# Spatial data
# - cell2fire expects all spatial data unrolled into columns of a csv with
#   a fixed set and order of columns
spatialData <- 
  tibble(
    fueltype = fuelsRaster[],
    mon = NA,
    jd = NA,
    M = NA,
    jd_min = NA,
    lat = NA,
    lon = NA,
    elev = if(!is.null(elevationRaster)){ elevationRaster[]} else NA,
    ffmc = NA,
    ws = NA,
    waz = NA,
    bui = NA,
    ps = NA,
    saz = NA,
    pc = NA_integer_,
    pdf = NA_integer_,
    gfl = NA_real_,
    cur = NA_integer_,
    time = NA,
    pattern = NA) %>%
  mutate(
    pc = lookup(fueltype, FuelType$ID, FuelType$PercentConifer),
    pdf = lookup(fueltype, FuelType$ID, FuelType$PercentDeadFir),
    gfl = lookup(fueltype, FuelType$ID, FuelType$GrassFuelLoading),
    cur = lookup(fueltype, FuelType$ID, FuelType$GrassCuring),
    fueltype = lookup(fueltype, FuelType$ID, FuelType$Code),
    fueltype = replace_na(fueltype, "NF"),
    elev = replace_na(elev, -9999)
  )

write_csv(spatialData, spatialDataFile, na = "")

# Convert ignition location to cell ID
ignitionLocation <- DeterministicIgnitionLocation %>%
  mutate(CellID = cellFromRowCol(fuelsRaster, Y, X)) %>%
  dplyr::select(Iteration, FireID, CellID) %>%
  arrange(Iteration, FireID) %>%
  mutate(UniqueFireID = row_number())

generateIgnitionFile(ignitionLocation$CellID)

# Split and save weather files
generateWeatherFiles(DeterministicBurnCondition) 

# Grow fires ----
progressBar("begin", totalSteps = length(iterations))
progressBar(type = "message", message = "Growing fires...")

runCell2Fire()

# Save relevant outputs ----
# Get relative paths to all raw outputs
rawOutputGridPaths <- file.path(gridOutputFolder, "Grids", 1:nrow(ignitionLocation), "ForestGrid00.csv")

## Fire statistics table ----
# Generate the table if it is a requested output, or resampling is requested
if(OutputOptions$FireStatistics | minimumFireSize > 0) {
  progressBar(type = "message", message = "Generating fire statistics table...")
  
  # Calculate burn areas for each fire
  burnAreas <- map_dbl(rawOutputGridPaths, getBurnArea) %>%
    `*`(raster::xres(fuelsRaster) * raster::yres(fuelsRaster) / 1e4)
  
  # Build fire statistics table
  OutputFireStatistic <-
    # Start by summarizing burn conditions
    DeterministicBurnCondition %>%
    
    # Only consider iterations this job is responsible for
    filter(Iteration %in% iterations) %>%
      
    # Summarize burn conditions by fire
    group_by(Iteration, FireID) %>%
    summarize(
      FireDuration = max(BurnDay),
      HoursBurning = sum(HoursBurning)) %>%
    ungroup() %>%
    mutate(Area = burnAreas) %>%
    
    # Append resampling info
    left_join(DeterministicIgnitionCount) %>%
    group_by(Iteration) %>%
    arrange(Iteration, FireID) %>%
    mutate(
        validFire = Area >= minimumFireSize,
        validFireCount = cumsum(validFire),
        ResampleStatus = case_when(
          !validFire                  ~ "Discarded",
          validFireCount <= Ignitions ~ "Kept",
          TRUE                        ~ "Not Used",
        )
    ) %>%
    ungroup()
    
  # Report status
  updateRunLog("\nBurn Summary:\n", 
               length(burnAreas), " fires burned. \n",
               sum(burnAreas < minimumFireSize, na.rm = T), " fires discarded due to insufficient burn area.\n",
               round(sum(burnAreas >= minimumFireSize, na.rm = T) / length(burnAreas) * 100, 0), "% of simulated fires were above the minimum fire size.\n",
               round(sum(OutputFireStatistic$ResampleStatus == "Not Used") / length(burnAreas) * 100, 0), "% of simulated fires not used because target ignition count was already met.")
  
  # Determine if target ignition counts were met for all iterations
  targetIgnitionsMet <- OutputFireStatistic %>%
    group_by(Iteration) %>%
    summarize(targetIgnitionsMet = max(validFireCount) >= head(Ignitions, 1)) %>%
    pull(targetIgnitionsMet)
  
  if(!all(targetIgnitionsMet))
    updateRunLog("\nCould not sample enough fires above the specified minimum fire size for ", sum(!targetIgnitionsMet),
                 " iterations. Please increase the Maximum Number of Fires to Resample per Iteration in the Run Controls",
                 " or decrease the Minimum Fire Size. Please see the Fire Statistics table for details on specific iterations,",
                 " fires, and burn conditions.", type = "warning")
  
 # Append extra info and save if requested 
  if(OutputOptions$FireStatistics | !all(targetIgnitionsMet)) {
    
    # Load necessary rasters and lookup tables
    fireZoneRaster <- tryCatch(
      datasheetRaster(myScenario, "burnP3Plus_LandscapeRasters", "FireZoneGridFileName"),
      error = function(e) NULL)
    weatherZoneRaster <- tryCatch(
      datasheetRaster(myScenario, "burnP3Plus_LandscapeRasters", "WeatherZoneGridFileName"),
      error = function(e) NULL)
    FireZoneTable <- datasheet(myScenario, "burnP3Plus_FireZone")
    WeatherZoneTable <- datasheet(myScenario, "burnP3Plus_WeatherZone")
    
    OutputFireStatistic <- OutputFireStatistic %>%
      
      # Determine Fire and Weather Zones if the rasters are present, as well as fuel type of ignition location
      left_join(DeterministicIgnitionLocation, by = c("Iteration", "FireID")) %>%
      mutate(
        cell = cellFromRowCol(fuelsRaster, Y, X),
        FireZone = ifelse(!is.null(fireZoneRaster), fireZoneRaster[cell] %>% lookup(FireZoneTable$ID, FireZoneTable$Name), ""),
        WeatherZone = ifelse(!is.null(weatherZoneRaster), weatherZoneRaster[cell] %>% lookup(WeatherZoneTable$ID, WeatherZoneTable$Name), ""),
        FuelType = fuelsRaster[cell] %>% lookup(FuelType$ID, FuelType$Name)) %>%
      
      # Incorporate Lat and Long and add TimeStep manually
      # - SyncoSim currently expects integers for X, Y, let's leave X, Y as Row, Col for now
      mutate(Timestep = 0) %>%
    
      # Clean up for saving
      dplyr::select(Iteration, Timestep, FireID, X, Y, Season, Cause, FireZone, WeatherZone, FuelType, FireDuration, HoursBurning, Area, ResampleStatus) %>%
      as.data.frame()
      
    # Output if there are records to save
    if(nrow(OutputFireStatistic) > 0)
      saveDatasheet(myScenario, OutputFireStatistic, "burnP3Plus_OutputFireStatistic", append = T)
  }
}

# Function to summarize individual burn grids by iteration
generateBurnAccumulators <- function(Iteration, UniqueFireIDs, burnGrids) {
  # initialize empty matrix
  accumulator <- matrix(0, nrow(fuelsRaster), ncol(fuelsRaster))
  
  # Combine burn grids
  for(i in UniqueFireIDs)
    if(!is.na(i))
      accumulator <- accumulator + as.matrix(read_csv(burnGrids[i], col_names = F, col_types = cols(.default = col_integer())))
  accumulator[accumulator != 0] <- 1
  
  # Mask and save as raster
  setValues(burnAccumulator, accumulator) %>%
    mask(fuelsRaster) %>%
    writeRaster(str_c(accumulatorOutputFolder, "/it", Iteration, ".tif"), overwrite = T, NAflag = -9999)
}

## Burn maps ----
if(saveBurnMaps) {
  progressBar(type = "message", message = "Saving burn maps...")
  
  # Identify which unique fire ID's belong to each iteration
  # - This depends on the resample status if fires are resampled, otherwise it is all fire IDs
  if(minimumFireSize >= 0) {
    ignitionsToExport <- ignitionLocation %>%
      left_join(OutputFireStatistic, by = c("Iteration", "FireID")) %>%
      filter(ResampleStatus == "Kept") %>%
      bind_rows(tibble(Iteration = iterations))
  } else
    ignitionsToExport <- ignitionLocation
  
  ignitionsToExport %>%
    dplyr::select(Iteration, UniqueFireID) %>%
    group_by(Iteration) %>%
    summarize(UniqueFireIDs = list(UniqueFireID)) %>%
    pwalk(generateBurnAccumulators, burnGrids = rawOutputGridPaths)
  
  # Build table of burn maps and save to SyncroSim
  OutputBurnMap <- 
    tibble(
      FileName = list.files(accumulatorOutputFolder, full.names = T) %>% normalizePath(),
      Iteration = str_extract(FileName, "\\d+.tif") %>% str_sub(end = -5) %>% as.integer(),
      Timestep = 0) %>%
    filter(Iteration %in% iterations) %>%
    as.data.frame
  
  # Output if there are records to save
  if(nrow(OutputBurnMap) > 0)
    saveDatasheet(myScenario, OutputBurnMap, "burnP3Plus_OutputBurnMap", append = T)
}

## Burn perimeters ----
if(OutputOptionsSpatial$BurnPerimeter) {
  warning("Cell2Fire does not provide burn perimeters.")
}

# Remove grid outputs if present
unlink(gridOutputFolder, recursive = T, force = T)
progressBar("end")
