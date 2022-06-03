# Burn-P3+ - Cell2Fire

This package is an add-on to the Burn-P3+ SyncroSim package that provides the
Cell2Fire fire growth model.

(base) root@c3ae76e4bb4a:/Cell2Fire/cell2fire# python main.py --help
cell2fire_path /Cell2Fire/cell2fire
usage: main.py [-h] [--input-instance-folder INFOLDER]
               [--output-folder OUTFOLDER] [--sim-years SIM_YEARS]
               [--nsims NSIMS] [--seed SEED] [--nweathers NWEATHERS]
               [--nthreads NTHREADS] [--max-fire-periods MAX_FIRE_PERIODS]
               [--IgnitionRad IGRADIUS] [--gridsStep GRIDSSTEP]
               [--gridsFreq GRIDSFREQ] [--heuristic HEURISTIC]
               [--MessagesPath MESSAGES_PATH] [--GASelection]
               [--HarvestedCells HCELLS] [--msgheur MSGHEUR]
               [--applyPlan PLANPATH] [--DFraction TFRACTION] [--GPTree]
               [--customValue VALUEFILE] [--noEvaluation] [--ngen NGEN]
               [--npop NPOP] [--tsize TSIZE] [--cxpb CXPB] [--mutpb MUTPB]
               [--indpb INDPB] [--weather WEATHEROPT] [--spreadPlots]
               [--finalGrid] [--verbose] [--ignitions] [--grids] [--simPlots]
               [--allPlots] [--combine] [--no-output] [--gen-data]
               [--output-messages] [--Prometheus-tuned] [--trajectories]
               [--stats] [--correctedStats] [--onlyProcessing] [--bbo]
               [--fdemand] [--pdfOutputs]
               [--Fire-Period-Length INPUT_PERIODLEN]
               [--Weather-Period-Length WEATHER_PERIOD_LEN]
               [--ROS-Threshold ROS_THRESHOLD] [--HFI-Threshold HFI_THRESHOLD]
               [--ROS-CV ROS_CV] [--HFactor HFACTOR] [--FFactor FFACTOR]
               [--BFactor BFACTOR] [--EFactor EFACTOR]
               [--BurningLen BURNINGLEN]

optional arguments:
  -h, --help            show this help message and exit
  --input-instance-folder INFOLDER
                        The path to the folder contains all the files for the
                        simulation
  --output-folder OUTFOLDER
                        The path to the folder for simulation output files
  --sim-years SIM_YEARS
                        Number of years per simulation (default 1)
  --nsims NSIMS         Total number of simulations (replications)
  --seed SEED           Seed for random numbers (default is 123)
  --nweathers NWEATHERS
                        Max index of weather files to sample for the random
                        version (inside the Weathers Folder)
  --nthreads NTHREADS   Number of threads to run the simulation
  --max-fire-periods MAX_FIRE_PERIODS
                        Maximum fire periods per year (default 1000)
  --IgnitionRad IGRADIUS
                        Adjacents degree for defining an ignition area (around
                        ignition point)
  --gridsStep GRIDSSTEP
                        Grids are generated every n time steps
  --gridsFreq GRIDSFREQ
                        Grids are generated every n episodes/sims
  --heuristic HEURISTIC
                        Heuristic version to run (-1 default no heuristic, 0
                        all)
  --MessagesPath MESSAGES_PATH
                        Path with the .txt messages generated for simulators
  --GASelection         Use the genetic algorithm instead of greedy selection
                        when calling the heuristic
  --HarvestedCells HCELLS
                        File with initial harvested cells (csv with year,
                        number of cells: e.g 1,1,2,3,4,10)
  --msgheur MSGHEUR     Path to messages needed for Heuristics
  --applyPlan PLANPATH  Path to Heuristic/Harvesting plan
  --DFraction TFRACTION
                        Demand fraction w.r.t. total forest available
  --GPTree              Use the Global Propagation tree for calculating the
                        VaR and performing the heuristic plan
  --customValue VALUEFILE
                        Path to Heuristic/Harvesting custom value file
  --noEvaluation        Generate the treatment plans without evaluating them
  --ngen NGEN           Number of generations for genetic algorithm
  --npop NPOP           Population for genetic algorithm
  --tsize TSIZE         Tournament size
  --cxpb CXPB           Crossover prob.
  --mutpb MUTPB         Mutation prob.
  --indpb INDPB         Individual prob.
  --weather WEATHEROPT  The 'type' of weather: constant, random, rows (default
                        rows)
  --spreadPlots         Generate spread plots
  --finalGrid           GGenerate final grid
  --verbose             Output all the simulation log
  --ignitions           Activates the predefined ignition points when using
                        the folder execution
  --grids               Generate grids
  --simPlots            generate simulation/replication plots
  --allPlots            generate spread and simulation/replication plots
  --combine             Combine fire evolution diagrams with the forest
                        background
  --no-output           Activates no-output mode
  --gen-data            Generates the Data.csv file before the simulation
  --output-messages     Generates a file with messages per cell, hit period,
                        and hit ROS
  --Prometheus-tuned    Activates the predefined tuning parameters based on
                        Prometheus
  --trajectories        Save fire trajectories FI and FS for MSS
  --stats               Output statistics from the simulations
  --correctedStats      Normalize the number of grids outputs for hourly stats
  --onlyProcessing      Read a previous simulation OutFolder and process it
                        (Cell2Fire simulation is not called)
  --bbo                 Use factors in BBOFuels.csv file
  --fdemand             Finer demand/treatment fraction
  --pdfOutputs          Generate pdf versions of all plots
  --Fire-Period-Length INPUT_PERIODLEN
                        Fire Period length in minutes (needed for ROS
                        computations). Default 60
  --Weather-Period-Length WEATHER_PERIOD_LEN
                        Weather Period length in minutes (needed weather
                        update). Default 60
  --ROS-Threshold ROS_THRESHOLD
                        A fire will not start or continue to burn in a cell if
                        the head ros is not above this value (m/min) default
                        0.1.
  --HFI-Threshold HFI_THRESHOLD
                        A fire will not start or continue to burn in a cell if
                        the HFI is not above this value (Kw/m) default is 10.
  --ROS-CV ROS_CV       Coefficient of Variation for normal random ROS (e.g.
                        0.13), but default is 0 (deteriministic)
  --HFactor HFACTOR     Adjustement factor: HROS
  --FFactor FFACTOR     Adjustement factor: FROS
  --BFactor BFACTOR     Adjustement factor: BROS
  --EFactor EFACTOR     Adjustement ellipse factor
  --BurningLen BURNINGLEN
                        Burning length period (periods a cell is burning)