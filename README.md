# CME_Parser
R Postgres client and settlement files parser
## Synopsis

This project deals with CME settlement files parse and uploads parsed data to Postgres database.

## Installation

Installation and database initialization is covered in my blog post http://mmquant.net/open-source-database-solution-for-daily-data-in-r/.

## Notice

This parser is intended to be an educational project. Use of CME settlement files for any other purposes without CME
permission is forbidden. If you want to use this project in real world application you have to use other source of daily
prices or contact CME about your intentions. Note that in practice analysts have prices coming from multiple data vendors
so they can cross-check their accuracy.

## Project structure overview

DBqueries directory contains sql statements for creating and populating foreign key tables. 
Data directory contains data files needed for running Parser object methods. Log directory hosts logfiles.
CreateTables.R, populating.R, tables_Triggers.R scripts
connects to the existing postgres database and execute sql queries from DBqueries directory. qGen.R generates input
rows for _symbol_ table.

## Contract months calendar maps (CMC)

In data directory there are CMC maps for every exchange which are used as guide for parser which symbols it downloads. Every CMC
file consists of _GLOBEX_symbol_, _Start_month_, _End_month_, _Start_day_ and _End_day_. You are free modify all of the columns except _Start_day_.


## Parser R6 Class overview and examples

- Public methods:
```sh
# Constructor
p <- Parser$new()
# Accesors
p$get("attribute")
p$set("attribute")
# parse() downloads and parses CME settlement reports
p$parse()
# exportQuery() exports parsed data to postgres database
p$exportQuery()
```
- Gettable attributes:  

| attribute | description |
| --- | --- |
| _logFile_ | Path to log file  |
| _reportDatePath_ | Path to last report date log file |
| _contractsCalPath_ | Path to contract months calendar files |
| _pricesPath_ |Path to downloaded settlement reports |
| _productMapPath_ | Path to products map file |
| _completeSymbs_ | Returns complete symbols to be downloaded (after parse() call) |
| _reportDate_ | Returns date of parsed settlement report (after parse() call) |
| _queryRows_ | Returns sql query rows to be exported into the database (after parse() call) |
| _db_ | Returns database connection settings - dbname, host, port, password |

- Settable attributes:  

| attribute | description |
| --- | --- |
| _logFile_ | Path to log file  |
| _reportDatePath_ | Path to last report date log file |
| _db_ | Sets database connection settings - list(dbname, host, port, password) |


## Possible upgrades
* Add different historical data sources

## Contributors

Petr Javorik http://mmquant.net maple@mmquant.net
