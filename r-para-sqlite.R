###############################################################################
### Using R and SQLite to process data using multiple cores from a database and 
### save it back into a different database
### Jesse Fagan
### October 12, 2014

### Clear the workspace
rm(list=ls())
gc()

### Libraries
library(data.table)
library(RSQLite)
library(parallel)
library(rbenchmark)
source('r-para-sqlite_functions.R')

### Functions #################################################################

con_data <- dbConnect(SQLite(), 'somedata.db')
con_result <- dbConnect(SQLite(), 'someresults.db')

n <- 1000
grps <- 1326
createDummyDB(con_data, n = n, grps = grps)

runAllModels()
runAllModelsMC(ncores = 4)

benchmark(runAllModels(), runAllModelsMC(ncores = 2), runAllModelsMC(ncores=6), runAllModelsMC(ncores=6, block.size = 500), runAllModelsMCDT(),
          order = 'relative', replications = 1)



dbDisconnect(con_data)
dbDisconnect(con_result)




