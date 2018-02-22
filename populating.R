### Connection
library(DBI)
library(RPostgreSQL)

drv <- dbDriver("PostgreSQL")
# con <- dbConnect(drv) # default connection for localhost
con <- dbConnect(drv,
                 dbname = "cme_data",
                 host = "192.168.88.202", # use PSQL server IP or "localhost"
                 port = 5432,
                 user = "r_client",
                 password = "yourPassword")

### Populating data_vendor table
fileName <- "DBqueries/populate_data_vendor_table.sql"
query <- readChar(fileName, file.info(fileName)$size)
dbExecute(con, query)

### Populating exchange table
fileName <- "DBqueries/populate_exchange_table.sql"
query <- readChar(fileName, file.info(fileName)$size)
dbExecute(con, query)

### Populating symbol_table
## Input file parse
out <- readLines("data/symbols.txt")
out1 <- gsub("[;'{}]", "", out[out != ""], fixed = FALSE)
out2 <- strsplit(out1," = ")

## Char to list
l <- vector("list",length(out2)/7)
rl <- 0
for (r in seq(1, length(out2), 7)) {
    cl <- 1
    rl <- rl+1
    for (c in seq(r, r+6, 1)) {
        l[[rl]][cl] <- out2[[c]][2]
        cl <- cl+1
    }
}
## List to dataframe
df <- do.call(rbind.data.frame, l)
colnames(df) <- c(
    "symbol",
    "months",
    "exchange",
    "name",
    "product_group",
    "currency",
    "born_year")
df <- data.frame(lapply(df, as.character), stringsAsFactors = FALSE)

## Generating INSERT INTO query
source("qGen.R")
source("twodigityears.R")
q <- apply(df, 1, qGen, fromYear = 1980, toYear = 2060) # set desired time frame
q1 <- unlist(q, recursive = FALSE)
q1[[length(q1)]] <- gsub(",$", ";", q1[[length(q1)]])
q2 <- c("INSERT INTO symbol (exchange_abbrev, instrument, name, product_group, currency,created, modified) VALUES", q1)
q3 <- paste(q2, collapse = "\n")
# write(q3,"q2Output.txt") # uncomment just for debugging purposes
dbExecute(con, q3)
# Disconnect from database
dbDisconnect(con)
