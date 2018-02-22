Parser <- R6Class(
    "Parser",
    public = list(
        
        ## Constructor
        initialize = function() {
            private$logFile <- "log/dataparse.log"
            private$reportDatePath <- "log/reportdate.log"
            private$cmmPath <- "data/Contract_months_map.csv"
            private$contractsCalPath <- list(
                "data/CBOT_contract_months_calendar.csv",
                "data/CME_contract_months_calendar.csv",
                "data/NYMEX_contract_months_calendar.csv",
                "data/COMEX_contract_months_calendar.csv"
            )
            private$pricesPath <- list(
                "data/stlags.txt",
                "data/stlcomex.txt",
                "data/stleqt.txt",
                "data/stlint.txt",
                "data/stlnymex.txt"
            )
            private$productMapPath <- list(
                "data/CBOT_products.csv",
                "data/CME_products.csv",
                "data/NYMEX_products.csv",
                "data/COMEX_products.csv"
            )
            private$db <- list(dbname = "cme_data",
                               host = NA,
                               port = NA,
                               user = 'r_client',
                               password = NA
            )
            private$dlUrl <- c("ftp://ftp.cmegroup.com/pub/settle/stlint",
                               "ftp://ftp.cmegroup.com/pub/settle/stleqt",
                               "ftp://ftp.cmegroup.com/pub/settle/stlags",
                               "ftp://ftp.cmegroup.com/pub/settle/stlnymex",
                               "ftp://ftp.cmegroup.com/settle/stlcomex"
            )
            private$destFile <- c("data/stlint.txt",
                                  "data/stleqt.txt",
                                  "data/stlags.txt",
                                  "data/stlnymex.txt",
                                  "data/stlcomex.txt"
            )

        },
        ## Accessors
        get = function(attrib) { 
            switch(attrib,
                   "logFile" = return(private$logFile),
                   "reportDatePath" = return(private$reportDatePath),
                   "contractsCalPath" = return(private$contractsCalPath),
                   "pricesPath" = return(private$pricesPath),
                   "productMapPath" = return(private$productMapPath),
                   "completeSymbs" = return(private$completeSymbs),
                   "reportDate" = return(private$reportDate),
                   "queryRows" = return(private$queryRows),
                   "db" = return(private$db),
                   "dlUrl" = return(private$dlUrl),
                   "destFile" = return(private$destFile),
                   "Valid attributes are: logfile, reportDatePath, contractsCalPath,
                   pricesPath, completeSymbs, reportDate, queryRows, db, dlUrl, destFile"
            )
        },
        set = function(attrib, value) { 
            switch(attrib,
                   "logFile" = private$logFile <- value,
                   "reportDatePath" = private$reportDatePath <- value,
                   "db" = private$db[c(2,3,5)] <- value,
                   "Valid attributes are: logFile, reportDatePath, db(argument is list(hostIP,port,password))"
            )
        },
        # Other methods
        parse = function() {
            private$logTime() # Logs parser execution
            private$dlData() # Downloads data
            private$loadPrices() # Loads downloaded prices
            private$checkDate() # Checks last vs. current report date
            private$symbGen() # Generates symbols for parse and insert to database
            private$sqlQuery() # Generates SQL query rows
        },
        # Exports data to database
        exportQuery = function() {
            drv <- dbDriver("PostgreSQL")
            # con <- dbConnect(drv) # default connection for localhost
            if (anyNA(private$db)) {
                return("Set db$host, db$port and db$password before query export. Example p$set(db,list(\"192.168.0.1\",5432,\"pass\")")
            }
            con <- dbConnect(drv,
                             dbname = private$db$dbname,
                             host = private$db$host,
                             port = private$db$port,
                             user = private$db$user,
                             password = private$db$password)
            
            # Query construction
            q1 <- apply(private$queryRows, 1, paste, collapse = ", ")
            q2 <- gsub("^(.*)$", "(\\1),", q1)
            q2[[length(q2)]] <- sub(",$",";",q2[[length(q2)]])
            q2 <- c("INSERT INTO daily_price(data_vendor_name,exchange_symbol,vendor_symbol,
                    complete_symbol,contract_month,price_date,open_price,high_price,low_price,
                    last_price,settle,volume,open_interest) VALUES",q2)
            q3 <- paste(q2, collapse = "\n")
            write(q3,"log/lastQuery.txt")
            dbExecute(con, q3)
            dbDisconnect(con)
        }
        
    ),
    private = list(
        
        ## Private attributes
        logFile = NA,
        reportDatePath = NA,
        contractsCalPath = NA,
        pricesPath = NA,
        cmmPath = NA, # contract months map
        productMapPath = NA,
        reportDate = NA,
        contractsCal = list(),
        prices = list(),
        completeSymbs = list(),
        queryRows = data.table(),
        dlUrl = NA,
        destFile = NA,
        maxDlTries = 5,
        # DB connection settings
        db = list(),
        
        ## Private methods
        # Loads contracts month calendar.
        loadContCal = function() {
            for (cal in 1:length(private$contractsCalPath))
                private$contractsCal[[cal]] <- fread(private$contractsCalPath[[cal]], header = T, sep = ',')
        },
        # Downloads data
        dlData = function () {
            cat("Downloading data:....")
            destfile <- c("stlint.txt","stleqt.txt","stlags.txt","stlnymex.txt","stlcomex.txt")
            t <- 0
            repeat {
                tryOut <- try(
                    download.file(private$dlUrl, private$destFile, method = "libcurl", quiet = TRUE, mode = "w", cacheOK = TRUE))
                t <- t+1
                if(!grepl(class(tryOut),"try-error") || t >= private$maxDlTries) {
                    break
                }
            }
            cat(" OK\n")
        },
        # Loads downloaded price files without options data.
        loadPrices = function() {
            # 1 data/stlags.txt
            # 2 data/stlcomex.txt
            # 3 data/stleqt.txt
            # 4 data/stlint.txt
            # 5 data/stlnymex.txt
            cat("Loading settlement files:")
            l <- vector("list",5)
            i <- 1
            for (f in private$pricesPath) {
                con  <- file(f, open = "r")
                j <- 1
                while (!grepl("CALL", oneLine <- readLines(con, n = 1))) {
                    l[[i]][[j]] <- oneLine
                    j <- j+1
                }
                close(con)
                i <- i+1
                cat(".")
            }
            private$prices <- l
            names(private$prices) <- c("stlags.txt", "stlcomex.txt", "stleqt.txt", "stlint.txt", "stlnymex.txt")
            cat(" OK\n")
        },
        # Checks and updates report date file.
        checkDate = function() {
            cat("Checking settlement reports date:...")
            # Last report date
            conOld  <- file(private$reportDatePath, open = "r+")
            dOld <- readLines(conOld, n = 1)
            # New report date
            conNew  <- file(private$pricesPath[[1]], open = "r")
            lNew <- readLines(conNew, n = 1)
            dNew <- str_extract(lNew, "\\d+/\\d+/\\d+")
            
            if (grepl(dOld,dNew)) {
                close(conOld)
                close(conNew)
                write("Report dates are equal. Stopping parser.", "log/dataparse.log" , append = TRUE)
                stop("Report dates are equal. Stopping parser.")
            }
            else {
                private$reportDate <- dNew
                writeLines(dNew,conOld)
                close(conOld)
                close(conNew)
                cat(" OK\n")
            }
        },
        # Core logic for selecting correct symbols according to the contract months calendar(CMC).
        symbGen = function() {
            cat("Generating symbols for download:.")
            # Enumerate contract months and days according to the CMC
            private$loadContCal()
            tDay <- as.numeric(unlist(strsplit(private$reportDate,"/")))[2]
            tMonth <- as.numeric(unlist(strsplit(private$reportDate,"/")))[1]
            tYear <- as.numeric(unlist(strsplit(private$reportDate,"/")))[3]
            
            dlSymbsId <- vector("list", length(private$contractsCal))
            dlSymbs <- vector("list", length(private$contractsCal))
            leapDlSymbs <- vector("list", length(private$contractsCal))
            completeSymbs <- vector("list", length(private$contractsCal))
            for (exchange in 1:length(private$contractsCal)) {
                
                calMat <- as.matrix(private$contractsCal[[exchange]][,2:5])
                tickers <- as.matrix(private$contractsCal[[exchange]][,1])
                l <- vector("list",nrow(calMat))
                
                for (r in 1:length(l)) {
                    l[[r]] <- list()
                    if (calMat[r,1] > calMat[r,3]) {
                        l[[r]][[1]] = c(seq(calMat[r,1],12), seq(1, calMat[r,3]))
                    } else {
                        l[[r]][[1]] = seq(calMat[r,1], calMat[r,3])
                    }
                    l[[r]][[2]] = seq(1, calMat[r,4])
                    # Find current trade month in enumerated months for given symbol (row)
                    tMid <- match(tMonth,l[[r]][[1]], nomatch = FALSE)
                    # 1st condition: If current trade month is the last enumerated trade month and trade day is in enumerated trade days
                    # 2nd condition : If current trade month is > 0 and not last enumerated trade month
                    if ( (tMid == length(l[[r]][[1]]) && tDay %in% l[[r]][[2]]) || (tMid > 0 && tMid < length(l[[r]][[1]])) ) {
                        # [[1]] CBOT
                        # [[2]] CME
                        # [[3]] NYMEX
                        # [[4]] COMEX
                        dlSymbsId[[exchange]][length(dlSymbsId[[exchange]]) + 1] <- r
                    }
                }
                cat(".")
                # Append trading year to symbols.
                # Leap symbol is symbol which is downloaded over the 1st January (in CMC Start_month > End_month).
                # If we are in months 1:5 we append current year to all leap symbols.
                # If we are in months 8:12 we append current year to all leap symbols.
                # There are no leap symbols starting in months 6:7.
                # NYMEX leap symbols have to be explicitly defined. See example from NYMEX CMC file below.
                #
                # GLOBEX_symbol,Start_month,Start_day,End_month,End_day
                # CLF,10,01,12,15
                # CLG,11,01,01,15
                # CLH,12,01,02,15
                #
                # Suppose we have tYear = X. Logic below appends correct year X+1 just to leap symbols
                # CLG and CLH but we need year X+1 even along CLF ! Clearly we can't use line ## 1 ##.
                
                dlSymbs[[exchange]] <- tickers[dlSymbsId[[exchange]],]
                if (exchange == 3) {
                    # NYMEX leap symbols
                    nymexLeaps <- rbind("HOF","HOG","HOH",
                                       "NGF","NGG","NGH",
                                       "CLF","CLG","CLH",
                                       "QMF","QMG","QMH",
                                       "QGF","QGG","QGH",
                                       "QGF","QGG","QGH",
                                       "RBF","RBG","RBH",
                                       "PAH",
                                       "PLF","PLJ")
                    leapDlSymbs[[exchange]] <- intersect(dlSymbs[[exchange]], nymexLeaps) ## 1 ##
                } else {
                    # CME, CBOT, COMEX leap symbols
                    leapDlSymbs[[exchange]] <- intersect(dlSymbs[[exchange]], tickers[calMat[,1] > calMat[,3]]) ## 1 ##
                }
                noLeapDlSymbs <- setdiff(dlSymbs[[exchange]], leapDlSymbs[[exchange]])
                noLp <- paste(noLeapDlSymbs, tYear, sep = "")
                cat(".")
                if (length(leapDlSymbs[[exchange]]) > 0 && length(noLeapDlSymbs) > 0) {
                    if (tMonth %in% 1:7) {
                        lp <- paste(leapDlSymbs[[exchange]], tYear, sep = "")
                    } else if (tMonth %in% 8:12) {
                        lp <- paste(leapDlSymbs[[exchange]], tYear + 1, sep = "")
                    }
                    completeSymbs[[exchange]] <- c(lp, noLp)
                    
                } else if (length(leapDlSymbs[[exchange]]) > 0 && length(noLeapDlSymbs) == 0) {
                    if (tMonth %in% 1:7) {
                        lp <- paste(leapDlSymbs[[exchange]], tYear, sep = "")
                    } else if (tMonth %in% 8:12) {
                        lp <- paste(leapDlSymbs[[exchange]], tYear + 1, sep = "")
                    }
                    completeSymbs[[exchange]] <- lp
                    
                } else
                    completeSymbs[[exchange]] <- noLp
            }
            write(paste("Downloaded symbols:",length(unlist(completeSymbs))), "log/dataparse.log", append = TRUE)
            private$completeSymbs <- completeSymbs
            cat(". OK\n")
        },
        # Generates SQL query.
        sqlQuery = function() {
            cat("Generating SQL query rows:.")
            # daily_price table columns:
            # data_vendor_name,exchange_symbol,vendor_symbol,complete_symbol,contract_month,price_date,O,H,L,Last,Settle,Volume,OI,created,modified
            complete_symbol <- as.matrix(unlist(private$completeSymbs))
            data_vendor_name <- as.matrix(rep("CME",times = length(complete_symbol)))
            exchange_symbol <- as.matrix(str_match(complete_symbol,"^([A-Z]+)[A-Z]")[,2])
            vendor_symbol <- exchange_symbol
            # contract_month in the form MMMYY
            y <- as.matrix(str_extract(complete_symbol,"\\d{2}"))
            mSymb <- as.matrix(str_match(complete_symbol,"([A-Z])\\d{2}")[,2])
            map <- read.csv("data/Contract_months_map.csv",stringsAsFactors = FALSE)
            mAbbr <- as.matrix(map[match(mSymb, map[,3]),1])
            contract_month <- as.matrix(paste(mAbbr, y, sep = ""))
            
            # price_date
            price_date <- as.matrix(private$reportDate)
            
            # OHLCV parse
            prodMap <- data.table()
            for (f in 1:length(private$productMapPath))
                prodMap <- rbind(prodMap,fread(private$productMapPath[[f]]), fill = TRUE)
            ids <- match(exchange_symbol,as.matrix(prodMap[,2]))
            currentProdMap <- prodMap[ids,]
            cat(".")
            # OHLCVI matrix  
            OHLCV <- matrix(nrow = nrow(currentProdMap), ncol = 7)
            colnames(OHLCV) <- c("O","H","L","Last","Settle","V","OI")
            for (r in 1:nrow(currentProdMap)) {
                settleFileDesc <- as.character(currentProdMap[r,4])
                settleFile <- private$prices[[as.character(currentProdMap[r,5])]]
                descId <- match(settleFileDesc, settleFile) # exact match, not matches substring in line
                MonthIds <- which(str_detect(settleFile, contract_month[r,1])) # matches substring in line
                line <- MonthIds[min(which(MonthIds > descId))]
                data <- settleFile[line]
                # Raw line consists of:
                # Month,Open,High,Low,Last,Settle,PCTchange,est.volume,...prior day info
                data <- unlist(str_extract_all(data,"[^\\s]+"))
                OHLCV[r,1:7] <- data[c(2,3,4,5,6,8,10)]
            }
            cat(".")
            # Cleaning the OHLCV matrix and conversion to double
            OHLCV <- sub("([.]*)([AB])$", "\\1", OHLCV)
            # Sorting out fraction values as follows:
            # Group 1: KE, ZC, ZO, ZS, ZW have minimum tick size 1/4 (For example 504'0 = 504, 504'2 = 504.25, 504'4 = 504.5, 504'6 = 504.75)
            # Group 2: UB, ZB, ZF, ZN, ZT have minimum tick size 1/32 or 1/64 (For example 157'27 = 157.84375, 157'20 = 157.625, ...)
            G1Id <- which(as.matrix(currentProdMap[,2]) %in% c("KE","ZC","ZO","ZS","ZW"))
            G2Id <- which(as.matrix(currentProdMap[,2]) %in% c("UB","ZB","ZF","ZN","ZT"))
            G1 <- matrix(unlist(lapply(OHLCV[G1Id,c(1:5)], private$frac2float, 1)), ncol = 5)
            G2 <- matrix(unlist(lapply(OHLCV[G2Id,c(1:5)], private$frac2float, 2)), ncol = 5)
            # return(list(G1Id = G1Id, G2Id = G2Id, G1 = G1, G2 = G2, OHLCV = OHLCV)) debug
            OHLCV[G1Id,1:5] <- G1
            OHLCV[G2Id,1:5] <- G2
            cat(".")
            # daily_price table columns:
            # data_vendor_name,exchange_symbol,vendor_symbol,complete_symbol,contract_month,price_date,O,H,L,Last,Settle,Volume,OI,created,modified
            private$queryRows <- data.table(data_vendor_name = data_vendor_name,
                                            exchange_symbol = exchange_symbol,
                                            vendor_symbol = vendor_symbol,
                                            complete_symbol = complete_symbol,
                                            contract_month = contract_month,
                                            price_date = price_date,
                                            Open = OHLCV[,1],
                                            High = OHLCV[,2],
                                            Low = OHLCV[,3],
                                            Last = OHLCV[,4],
                                            Settle = OHLCV[,5],
                                            Volume = OHLCV[,6],
                                            OpenInterest = OHLCV[,7])
            cat(".")
            # Single quote strings
            private$queryRows[,1:6] <- lapply(private$queryRows[,1:6], function(x) { gsub("^(.*)$", "'\\1'", x) })
            # '---' values to 0
            private$queryRows[,7:13] <- lapply(private$queryRows[,7:13], function(x) { str_replace(x,"----","0") })
            cat(".")
            cat(" OK\n")
        },
        # Logs system time to logfile
        logTime = function() {
            t <- paste("-------------------------------------------------------\n",as.character(Sys.time()), sep = "")
            write(t, private$logFile, append = TRUE)
        },
        # Conversion function converts fractional prices in settlement file. Called by sqlQuery.
        frac2float = function(OHLCSfrac, group) {
            # OHLCSfrac is just 1x5 matrix ! To be called with apply
            # OHLCSfrac - Open, High, Low, Close, Settle
            # group -   1 = "KE","ZC","ZO","ZS","ZW"
            #           2 = "UB","ZB","ZF","ZN","ZT"
            if (group == 1) {
                temp <- str_split(OHLCSfrac,"'", simplify = TRUE)
                class(temp) <- "numeric"
                OHLCSfloat <- as.matrix(temp[,1] + temp[,2]/8)
                class(OHLCSfloat) <- "character"
            } else if (group == 2) {
                temp <- str_split(OHLCSfrac,"'", simplify = TRUE)
                # Correction for bond fraction. 24 converts to 240 etc. We need to divide all fractional parts from group 2 by 320
                if (nchar(temp[1,2]) < 3) {
                    mult <- 10
                } else {
                    mult <- 1
                }
                class(temp) <- "numeric"
                OHLCSfloat <- as.matrix(temp[,1] + temp[,2]/320*mult)
                class(OHLCSfloat) <- "character"
            }
            
            return(OHLCSfloat)
        }
    )
)
