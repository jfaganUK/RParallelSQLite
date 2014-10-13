createDummyDB <- function(con, n=1000, grps=1000) {
  
  # drop the table if it already exists
  dbGetQuery(con, 'drop table if exists somedata')
  
  # create a table
  dbGetQuery(con, 'create table if not exists somedata 
             (id integer primary key autoincrement,
             x real, y real, grp integer)')
  
  dbGetQuery(con, 'create index iGrp on somedata (grp)')
  
  # create the data
  x <- data.table(x=rnorm(grps*n), y=runif(grps*n), grp=rep(1:grps, n))
  
  # insert into table
  dbBeginTransaction(con)
  res <- dbSendQuery(con, 'insert into somedata (x, y, grp) values (?, ?, ?)', 
                     bind.data=x)
  dbCommit(con)
}

createOutputDB <- function(con) {
  dbGetQuery(con, 'drop table if exists someresults')
  dbGetQuery(con, 'create table if not exists someresults 
             (grp integer primary key,
             intercept real, xcoef real, r2 real, pval real)')
  dbGetQuery(con, 'create index iGrp on someresults (grp)')
}

getDataByGroup <- function(con, grp) {
  d <- dbGetQuery(con, 'select * from somedata where grp = ?', grp)
  return(d)
}

runModel <- function(d) {
  lm0 <- lm(y ~ x, data=d)
  lm0.sum <- summary(lm0)
  lm0.pval <- pf(lm0.sum$fstatistic[1], lm0.sum$fstatistic[2], lm0.sum$fstatistic[3], lower.tail = F)
  d.result <- data.table(grp = min(d$grp),
                         intercept = lm0$coefficients[1],
                         xcoef = lm0$coefficients[2],
                         r2 = lm0.sum$r.squared,
                         pval = lm0.pval)
  return(d.result)
}

insertResult <- function(con, d.result) {
  res <- dbSendQuery(con, 'insert into someresults (grp, intercept, xcoef, r2, pval) values
                     (:grp, :intercept, :xcoef, :r2, :pval)', bind.data = as.data.frame(d.result))
}

# assumes globals con_result, con_data, n, grps
runAllModels <- function() {
  createOutputDB(con_result)
  for(i in 1:grps) {
    d <- getDataByGroup(con_data, i)
    d.result <- runModel(d)
    insertResult(con_result, d.result)
  }
}


buildCommaList <- function(x) {
  if(class(x) %in% c("integer", "numeric")) {
    cl <- paste("(",paste(x, collapse=","),")",sep="")    
  } else {
    cl <- paste("(`",paste(x, collapse="`,`"),"`)",sep="")    
  }
  cl
}

buildGrpList <- function(all.grps, i, block.size) {
  b1 <- i*block.size + 1
  b2 <- (i+1)*block.size
  if(b2 > length(all.grps)) {
    b2 <- length(all.grps)
  }
  l <- all.grps[b1:b2]
}

### SQLite Multicore
# ncores      = the number of cores
# block.size  = the number of records that are pulled from the database
# There is a big problem with concurrency.
# For write concurrency, I figure I will just save everything to memory and then write it to the db in big commit.
# For the read concurrency...

runAllModelsMC <- function(ncores = 4, block.size = 100) {
  
  foo <- function(i, d) {
    d0 <- d[grp == i]
    runModel(d0)
  }
  
  createOutputDB(con_result)
  
  all.grps <- 1:grps
  i <- 0
  l <- numeric(block.size)
  while(length(l) == block.size) {
    
    # grab a block.size subset of the database
    l <- buildGrpList(all.grps, i, block.size)
    i <- i + 1
    sql <- paste('select * from somedata where grp in ', buildCommaList(l),sep="")
    d <- data.table(dbGetQuery(con_data, sql))
    
    # do a multicore analysis on each group
    d.result <- do.call('rbind', mclapply(l, function(x) { foo(x, d) }, mc.cores = ncores))
    
    # insert the results
    dbBeginTransaction(con_result)
    insertResult(con_result, d.result)
    dbCommit(con_result)
  }
}

runAllModelsMCDT <- function(ncores = 4) {
  createOutputDB(con_result)
  d <- data.table(dbGetQuery(con_data, 'select * from somedata'))
  
  foo <- function(i, d) {
    d0 <- d[grp == i]
    runModel(d0)
  }
  
  d.result <- do.call('rbind', mclapply(1:grps, function(x) { foo(x, d) }, mc.cores = ncores))
  
  dbBeginTransaction(con_result)
  insertResult(con_result, d.result)
  dbCommit(con_result)
}