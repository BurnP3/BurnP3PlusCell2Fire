library(rsyncrosim)
suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(terra))
suppressPackageStartupMessages(library(data.table))

# Setup ----
progressBar(type = "message", message = "Preparing inputs...")

# Initialize first breakpoint for timing code
currentBreakPoint <- proc.time()

## Connect to SyncroSim ----

myScenario <- scenario()

# Load Run Controls and identify iterations to run
RunControl <- datasheet(myScenario, "burnP3Plus_RunControl")
iterations <- seq(RunControl$MinimumIteration, RunControl$MaximumIteration)

# Load remaining datasheets
BatchOption <- datasheet(myScenario, "burnP3PlusCell2Fire_BatchOption")
ResampleOption <- datasheet(myScenario, "burnP3Plus_FireResampleOption")
DeterministicIgnitionCount <- datasheet(myScenario, "burnP3Plus_DeterministicIgnitionCount", lookupsAsFactors = F, optional = T) %>% unique %>% filter(Iteration %in% iterations)
DeterministicIgnitionLocation <- datasheet(myScenario, "burnP3Plus_DeterministicIgnitionLocation", lookupsAsFactors = F, optional = T) %>% unique %>% filter(Iteration %in% iterations)
DeterministicBurnCondition <- datasheet(myScenario, "burnP3Plus_DeterministicBurnCondition", lookupsAsFactors = F, optional = T) %>% unique %>% filter(Iteration %in% iterations)
FuelType <- datasheet(myScenario, "burnP3Plus_FuelType", lookupsAsFactors = F)
FuelTypeCrosswalk <- datasheet(myScenario, "burnP3PlusCell2Fire_FuelCodeCrosswalk", lookupsAsFactors = F, optional = T)
ValidFuelCodes <- datasheet(myScenario, "burnP3PlusCell2Fire_FuelCode") %>% pull()
WindGrid <- datasheet(myScenario, "burnP3Plus_WindGrid", lookupsAsFactors = F, optional = T)
GreenUp <- datasheet(myScenario, "burnP3Plus_GreenUp", lookupsAsFactors = F, optional = T)
Curing <- datasheet(myScenario, "burnP3Plus_Curing", lookupsAsFactors = F, optional = T)
FuelLoad <- datasheet(myScenario, "burnP3Plus_FuelLoad", lookupsAsFactors = F, optional = T)
OutputOptions <- datasheet(myScenario, "burnP3Plus_OutputOption", optional = T)
OutputOptionsSpatial <- datasheet(myScenario, "burnP3Plus_OutputOptionSpatial", optional = T)

# Import relevant rasters, allowing for missing elevation
fuelsRaster <- rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["FuelGridFileName"]])
elevationRaster <- tryCatch(
  rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["ElevationGridFileName"]]),
  error = function(e) NULL)

## Handle empty values ----
if(nrow(FuelTypeCrosswalk) == 0) {
  updateRunLog("No fuels code crosswalk found! Using default crosswalk for Canadian Forest Service fuel codes.", type = "warning")
  FuelTypeCrosswalk <- fread(file.path(ssimEnvironment()$PackageDirectory, "Default Fuel Crosswalk.csv"))
  saveDatasheet(myScenario, as.data.frame(FuelTypeCrosswalk), "burnP3PlusCell2Fire_FuelCodeCrosswalk")
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

if(nrow(BatchOption) == 0) {
  updateRunLog("No batch size chosen. Defaulting to batches of 250 iterations.", type = "info")
  BatchOption[1,] <- c(250)
  saveDatasheet(myScenario, BatchOption, "burnP3PlusCell2Fire_BatchOption")
}

if(nrow(ResampleOption) == 0) {
  ResampleOption[1,] <- c(0,0)
  saveDatasheet(myScenario, ResampleOption, "burnP3Plus_FireResampleOption")
}

# Handle unsupported inputs
if(nrow(WindGrid) != 0) {
  updateRunLog("Cell2Fire currently does not support Wind Grids. Wind Grid options ignored.", type = "warning")
}

if(nrow(GreenUp) != 0) {
  updateRunLog("Cell2Fire transformer currently does not support Green Up. Green Up options ignored.", type = "warning")
}

if(nrow(Curing) != 0) {
  updateRunLog("Cell2Fire transformer currently does not support specifying Curing by season. Please use the Cell2Fire Fuel Code Crosswalk to statically specify curing by fuel type.", type = "warning")
}

if(nrow(FuelLoad) != 0) {
  updateRunLog("Cell2Fire transformer currently does not support specifying Fuel Loading by season. Please use the Cell2Fire Fuel Code Crosswalk to statically specify fuel loading by fuel type.", type = "warning")
}

## Check raster inputs for consistency ----

# Ensure fuels crs can be converted to Lat / Long
tryCatch(fuelsRaster %>% project("+proj=longlat"), error = function(e) stop("Error parsing provided Fuels map. Cannot calculate Latitude and Longitude from provided Fuels map, please check CRS."))

# Define function to check input raster for consistency
checkSpatialInput <- function(x, name, checkProjection = T, warnOnly = F) {
  # Only check if not null
  if(!is.null(x)) {
    # Ensure comparable number of rows and cols in all spatial inputs
      if(nrow(fuelsRaster) != nrow(x) | ncol(fuelsRaster) != ncol(x))
        if(warnOnly) {
          updateRunLog("Number of rows and columns in ", name, " map do not match Fuels map. Please check that the extent and resolution of these maps match.", type = "warning")
          invisible(NULL)
        } else
          stop("Number of rows and columns in ", name, " map do not match Fuels map. Please check that the extent and resolution of these maps match.")
    
    # Info if CRS is not matching
    if(checkProjection)
      if(crs(x) != crs(fuelsRaster))
        updateRunLog("Projection of ", name, " map does not match Fuels map. Please check that the CRS of these maps match.", type = "info")
  }
  
  # Silently return for clean pipelining
  invisible(x)
}

# Check optional inputs
checkSpatialInput(elevationRaster, "Elevation")

## Extract relevant parameters ----

# Batch sizes to use to limit disk usage of output files
batchSize <- BatchOption$BatchSize

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
fuelIdsPresent <- fuelsRaster %>% unique() %>% pull
if(any(!fuelIdsPresent %in% c(FuelType$ID, NaN)))
  stop("Found one or more values in the Fuels Map that are not assigned to a known Fuel Type. Please add definitions for the following Fuel IDs: ", 
       dplyr::setdiff(fuelIdsPresent, data.frame(Fuels = FuelType[,"ID"])) %>% str_c(collapse = " "))

## Setup files and folders ----

# Copy Cell2Fire executable
setwd(ssimEnvironment()$TempDirectory)

# Select the appropriate executable for the system OS
if(.Platform$OS.type == "unix") {
  cell2fireExecutable <- "./Cell2Fire"
} else {
  cell2fireExecutable <- "Cell2Fire.exe"
}
file.copy(file.path(ssimEnvironment()$PackageDirectory, cell2fireExecutable), ssimEnvironment()$TempDirectory, overwrite = T)

# Set as executable if in linux
if(.Platform$OS.type == "unix")
  system2("chmod", c("+x", cell2fireExecutable))
  
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


## Function Definitions ----

### Convenience and conversion functions ----

# Function to time code by returning a clean string of time since this function was last called
updateBreakpoint <- function() {
  # Calculate time since last breakpoint
  newBreakPoint <- proc.time()
  elapsed <- (newBreakPoint - currentBreakPoint)['elapsed']
  
  # Update current breakpoint
  currentBreakPoint <<- newBreakPoint
  
  # Return cleaned elapsed time
  if (elapsed < 60) {
    return(str_c(round(elapsed), "sec"))
  } else if (elapsed < 60^2) {
    return(str_c(round(elapsed / 60, 1), "min"))
  } else
    return(str_c(round(elapsed / 60 / 60, 1), "hr"))
}

# Define a function to facilitate recoding values using a lookup table
lookup <- function(x, old, new) dplyr::recode(x, !!!set_names(new, old))

# Function to delete files in file
resetFolder <- function(path) {
  list.files(path, full.names = T) %>%
    unlink(recursive = T, force = T)
  invisible()
}

# Function to convert from latlong to cell index
cellFromLatLong <- function(x, lat, long) {
  # Convert list of lat and long to SpatVector, reproject to source crs
  points <- matrix(c(long, lat), ncol = 2) %>%
    vect(crs = "+proj=longlat +datum=WGS84 +no_defs") %>%
    project(x)
  
  # Get vector of cell ID's from points
  return(cells(x, points)[, "cell"])
}

# Get burn area from output csv
getBurnArea <- function(inputFile) {
  fread(inputFile, header = F) %>%
    as.matrix() %>%
    sum %>%
    return
}

# Get burn areas from all generated output files
getBurnAreas <- function(rawOutputGridPaths) {
  # Calculate burn areas for each fire
  burnAreas <- c(NA_real_)
  length(burnAreas) <- length(rawOutputGridPaths)

  burnAreas <- unlist(lapply(rawOutputGridPaths[seq_along(burnAreas)],getBurnArea))

  # Convert pixels to hectares (resolution is assumed to be in meters)
  burnAreas <- burnAreas * (xres(fuelsRaster) * yres(fuelsRaster) / 1e4)
  
  return(burnAreas)
}

# Function to determine which fires should be kept after resampling
getResampleStatus <- function(burnSummary) {
  if(minimumFireSize > 0) {
    burnSummary %>%
      left_join(DeterministicIgnitionCount, by = "Iteration") %>%
      group_by(Iteration) %>%
      arrange(Iteration, FireID) %>%
      mutate(
          validFire = Area >= minimumFireSize,
          validFireCount = cumsum(validFire),
          ResampleStatus = case_when(
            !validFire                  ~ "Discarded",
            validFireCount <= Ignitions ~ "Kept",
            TRUE                        ~ "Not Used",
          )) %>%
      ungroup() %>%
      select(Iteration, FireID, UniqueFireID, Area, ResampleStatus) %>%
      return()
  } else {
    burnSummary %>%
      mutate(ResampleStatus = "Kept") %>%
      return()
  }
}

# Function to convert, accumulate, and clean up raw outputs
processOutputs <- function(batchOutput, rawOutputGridPaths) {
  # Identify which unique fire ID's belong to each iteration
  # - bind_rows is used to ensure iterations aren't lost if all fires in an iteration are discarded due to size
  batchOutput <- batchOutput %>%
    filter(ResampleStatus == "Kept") %>%
    bind_rows(tibble(Iteration = unique(batchOutput$Iteration)))
    
  # Summarize the FireIDs to export by Iteration
  ignitionsToExportTable <- batchOutput %>%
    dplyr::select(Iteration, UniqueFireID) %>%
    group_by(Iteration) %>%
    summarize(UniqueFireIDs = list(UniqueFireID))
  
  # Generate burn count maps
  for (i in seq_len(nrow(ignitionsToExportTable)))
    generateBurnAccumulators(Iteration = ignitionsToExportTable$Iteration[i], UniqueFireIDs = ignitionsToExportTable$UniqueFireIDs[[i]], burnGrids = rawOutputGridPaths)
}

# Function to call Cell2Fire on the (global) parameter file
runCell2Fire <- function(numIgnitions) {
  resetFolder(gridOutputFolder)
  
  # Format folder paths if on windows
  if(.Platform$OS.type == "unix") {
    inputInstanceFolder <- tempDir %>%
      str_c("/")
    outputFolder <- gridOutputFolder %>%
      str_c("/")  
  } else {
    inputInstanceFolder <- tempDir %>%
      str_c("\\\\")
    outputFolder <- gridOutputFolder %>%
      str_c("\\\\")  
  }
  
  system2(cell2fireExecutable,
          c("--input-instance-folder", inputInstanceFolder,
            "--output-folder", outputFolder,
            "--nsims", numIgnitions,
            "--nweathers", numIgnitions,
            "--ignitions --final-grid --Fire-Period-Length 1.0 --weather sequential"))
}

# Function to run one batch of iterations
runBatch <- function(batchInputs) {
  # Generate batch-specific inputs
  # - Unnest and process ignition info
  batchInputs <- unnest(batchInputs, data)
  generateIgnitionFile(batchInputs$CellID)
  numIgnitions <- nrow(batchInputs)
  
  # - Unnest and process weather info
  batchWeather <- unnest(batchInputs, data)
  generateWeatherFiles(batchWeather)
  
  # Run Cell2Fire on the batch
  runCell2Fire(numIgnitions)
  
  # Get relative paths to all raw outputs
  rawOutputGridPaths <- file.path(gridOutputFolder, "Grids", seq(numIgnitions), "ForestGrid00.csv")
  
  # Get burn areas
  burnAreas <- getBurnAreas(rawOutputGridPaths)
  
  # Convert and save spatial outputs as needed
  batchOutput <- batchInputs %>%
    select(Iteration, FireID) %>%
    mutate(
      UniqueFireID = row_number(),
      Area = burnAreas) %>%
    getResampleStatus()
    
  # Save GeoTiffs if needed
  if(saveBurnMaps)
    processOutputs(batchOutput, rawOutputGridPaths)
  
  # Clear up temp files
  resetFolder(gridOutputFolder)
  
  # Update Progress Bar
  progressBar("step")
  progressBar(type = "message", message = "Growing fires...")
  
  # Return relevant outputs
  batchOutput %>%
    select(-UniqueFireID) %>%
    return()
}

### File generation functions ----

# Function to generate empty weather template file required by C2F
generateWeatherTemplateFile <- function() {
  data.table(
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
  fwrite(file.path(tempDir, "Weather.csv"), na = "")
  invisible()
}

# Function to convert daily weather data for every day of burning to format
# expected by C2F and save to file
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
    fwrite(file.path(weatherFolder, str_c("Weather", uniqueFireIndex, ".csv")))
  invisible()
}

# Function to split deterministic burn conditions into separate weather files by iteration and fire id
generateWeatherFiles <- function(DeterministicBurnCondition){
  # Clear out old weather files if present
  resetFolder(weatherFolder)
  
  # Generate files as needed
  DeterministicBurnCondition %>%
    group_by(Iteration, FireID) %>%
    nest() %>%
    ungroup() %>%
    arrange(Iteration, FireID) %>%
    transmute(weatherData = data, uniqueFireIndex = row_number()) %>%
    pmap(generateWeatherFile)
  invisible()
}

# Function to create an ignition location file
generateIgnitionFile <- function(CellIDs){
  unlink(ignitionFile, force = T)
  data.table(Year = seq_along(CellIDs), Ncell = CellIDs) %>%
    fwrite(ignitionFile)
}

# Function to summarize individual burn grids by iteration
generateBurnAccumulators <- function(Iteration, UniqueFireIDs, burnGrids) {
  # initialize empty matrix
  accumulator <- matrix(0, nrow(fuelsRaster), ncol(fuelsRaster))
  
  # Combine burn grids
  for(i in UniqueFireIDs)
    if(!is.na(i))
      burnArea <- as.matrix(fread(burnGrids[i],header = F)
      
      ## TODO: Define allPerims in Spatial output options.
      if(allPerims == T){
        rast(fuelsRaster, vals = burnArea) %>% 
          mask(fuelsRaster) %>%
            writeRaster(str_c(accumulatorOutputFolder, "/it", Iteration,"_fire_", UniqueFireID, ".tif"), 
                overwrite = T,
                NAflag = -9999,
                wopt = list(filetype = "GTiff",
                     datatype = "INT4S",
                     gdal = c("COMPRESS=DEFLATE","ZLEVEL=9","PREDICTOR=2")))}
                     
      accumulator <- accumulator + burnArea)
  accumulator[accumulator != 0] <- 1
  
  # Mask and save as raster
  rast(fuelsRaster, vals = accumulator) %>%
    mask(fuelsRaster) %>%
    writeRaster(str_c(accumulatorOutputFolder, "/it", Iteration, ".tif"), 
                overwrite = T,
                NAflag = -9999,
                wopt = list(filetype = "GTiff",
                     datatype = "INT4S",
                     gdal = c("COMPRESS=DEFLATE","ZLEVEL=9","PREDICTOR=2")))
}


updateRunLog("Finished parsing run inputs in ", updateBreakpoint())

# Prepare Shared Inputs ----

# Raster metadata
# - cell2fire uses the Forest.asc exclusively to read metadata
# - to save disk space we only write metatdata to this file
mapMetadata <- str_c("ncols ",        ncol(fuelsRaster),    "\n",
                     "nrows ",        nrow(fuelsRaster),    "\n",
                     "xllcorner ",    xmin(fuelsRaster),    "\n",
                     "yllcorner ",    ymin(fuelsRaster),    "\n",
                     "cellsize ",     res(fuelsRaster)[1],  "\n",
                     "NODATA_value ", NAflag(fuelsRaster), "\n")

write_file(mapMetadata, mapMetadataFile)

# Spatial data
# - cell2fire expects all spatial data unrolled into columns of a csv with
#   a fixed set and order of columns
spatialData <- 
  tibble(
    fueltype = values(fuelsRaster, mat = F),
    mon = NA,
    jd = NA,
    M = NA,
    jd_min = NA,
    lat = NA,
    lon = NA,
    elev = if(!is.null(elevationRaster)){ values(elevationRaster, mat = F)} else NA,
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
fwrite(spatialData, spatialDataFile, na = "")

# Convert ignition location to cell ID
ignitionLocation <- DeterministicIgnitionLocation %>%
  mutate(CellID = cellFromLatLong(fuelsRaster, Latitude, Longitude)) %>%
  dplyr::select(Iteration, FireID, CellID) %>%
  arrange(Iteration, FireID)

# Generate empty weather template file
generateWeatherTemplateFile()

# Combine deterministic input tables ----
fireGrowthInputs <- DeterministicBurnCondition %>%
  # Only consider iterations this job is responsible for
  filter(Iteration %in% iterations) %>%
  
  # Group by iteration and fire ID for the `growFire()` function
  group_by(Iteration, FireID) %>%
  nest %>%
  
  # Add ignition location information
  left_join(ignitionLocation, c("Iteration", "FireID")) %>%
  
  # Group by just iteration for the `runIteration()` function
  group_by(Iteration) %>%
  nest %>%
  
  # Finally split into batches of the appropriate size
  group_by(batchID = (row_number() - 1) %/% batchSize) %>%
  group_split(.keep = F)

updateRunLog("Finished generating shared inputs in ", updateBreakpoint())

# Grow fires ----
progressBar("begin", totalSteps = length(fireGrowthInputs))
progressBar(type = "message", message = "Growing fires...")

OutputFireStatistic <- fireGrowthInputs %>%
  map_dfr(runBatch)

# Report status ----
updateRunLog("\nBurn Summary:\n", 
             nrow(OutputFireStatistic), " fires burned. \n",
             sum(OutputFireStatistic$ResampleStatus == "Discarded"), " fires discarded due to insufficient burn area.\n",
             round(sum(OutputFireStatistic$Area >= minimumFireSize) / nrow(OutputFireStatistic) * 100, 0), "% of simulated fires were above the minimum fire size.\n",
             round(sum(OutputFireStatistic$ResampleStatus == "Not Used") / nrow(OutputFireStatistic) * 100, 0), "% of simulated fires not used because target ignition count was already met.\n")

# Determine if target ignition counts were met for all iterations
targetIgnitionsMet <- OutputFireStatistic %>%
  left_join(DeterministicIgnitionCount, by = "Iteration") %>%
  group_by(Iteration) %>%
  summarize(targetIgnitionsMet = sum(ResampleStatus == "Kept") >= head(Ignitions, 1)) %>%
  pull(targetIgnitionsMet)

if(!all(targetIgnitionsMet))
  updateRunLog("Could not sample enough fires above the specified minimum fire size for ", sum(!targetIgnitionsMet),
               " iterations. Please increase the Maximum Number of Fires to Resample per Iteration in the Run Controls",
               " or decrease the Minimum Fire Size. Please see the Fire Statistics table for details on specific iterations,",
               " fires, and burn conditions.\n", type = "warning")
  
updateRunLog("Finished burning fires in ", updateBreakpoint())

# Save relevant outputs ----

## Fire statistics table ----
# Generate the table if it is a requested output, or resampling is requested
if(OutputOptions$FireStatistics | !all(targetIgnitionsMet)) {
  progressBar(type = "message", message = "Generating fire statistics table...")
  
  # Load necessary rasters and lookup tables
  fireZoneRaster <- tryCatch(
    rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["FireZoneGridFileName"]]),
    error = function(e) NULL) %>%
    checkSpatialInput("Fire Zone", warnOnly = T)
  weatherZoneRaster <- tryCatch(
    rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["WeatherZoneGridFileName"]]),
    error = function(e) NULL) %>%
    checkSpatialInput("Weather Zone", warnOnly = T)
  FireZoneTable <- datasheet(myScenario, "burnP3Plus_FireZone")
  WeatherZoneTable <- datasheet(myScenario, "burnP3Plus_WeatherZone")
    
  # Add extra information to Fire Statistic table
  OutputFireStatistic <- OutputFireStatistic %>%
    
    # Start by joining summarized burn conditions
    left_join({
      # Start by summarizing burn conditions
      DeterministicBurnCondition %>%
      
        # Only consider iterations this job is responsible for
        filter(Iteration %in% iterations) %>%
          
        # Summarize burn conditions by fire
        group_by(Iteration, FireID) %>%
        summarize(
          FireDuration = max(BurnDay),
          HoursBurning = sum(HoursBurning)) %>%
        ungroup()},
      by = c("Iteration", "FireID")) %>%
  
      # Determine Fire and Weather Zones if the rasters are present, as well as fuel type of ignition location
      left_join(DeterministicIgnitionLocation, by = c("Iteration", "FireID")) %>%
      mutate(
        cell = cellFromLatLong(fuelsRaster, Latitude, Longitude),
        FireZone = ifelse(!is.null(fireZoneRaster), fireZoneRaster[][cell] %>% lookup(FireZoneTable$ID, FireZoneTable$Name), ""),
        WeatherZone = ifelse(!is.null(weatherZoneRaster), weatherZoneRaster[][cell] %>% lookup(WeatherZoneTable$ID, WeatherZoneTable$Name), ""),
        FuelType = fuelsRaster[][cell] %>% lookup(FuelType$ID, FuelType$Name)) %>%
      
      # Incorporate Lat and Long and add TimeStep manually
      mutate(Timestep = 0) %>%
    
      # Clean up for saving
      dplyr::select(Iteration, Timestep, FireID, Latitude, Longitude, Season, Cause, FireZone, WeatherZone, FuelType, FireDuration, HoursBurning, Area, ResampleStatus) %>%
      as.data.frame()
      
    # Output if there are records to save
    if(nrow(OutputFireStatistic) > 0)
      saveDatasheet(myScenario, OutputFireStatistic, "burnP3Plus_OutputFireStatistic", append = T)
  
  updateRunLog("Finished collecting fire statistics in ", updateBreakpoint())
}

## Burn maps ----
if(saveBurnMaps) {
  progressBar(type = "message", message = "Saving burn maps...")
  
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
  
  updateRunLog("Finished accumulating burn maps in ", updateBreakpoint())
}

## Burn perimeters ----
if(OutputOptionsSpatial$BurnPerimeter) {
  updateRunLog("Cell2Fire does not provide burn perimeters.", type = "info")
}

# Clean up
progressBar("end")
