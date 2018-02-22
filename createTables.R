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

## Create tables
fileName <- "DBqueries/create_tables.sql"
query <- readChar(fileName, file.info(fileName)$size)
dbExecute(con, query)
## Create function trigger
fileName <- "DBqueries/update_trigger.sql"
query <- readChar(fileName, file.info(fileName)$size)
dbExecute(con, query)
## Check created tables and field names
tables <- dbListTables(con)
for (i in 1:length(tables)) {
    cat(dbListFields(con, tables[i]))
    cat("\n-----------------------------------------------------------------------------\n")
}
# Disconnect from database
dbDisconnect(con)