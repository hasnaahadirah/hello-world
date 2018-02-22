qGen <- function(dfRowIn, fromYear, toYear) {
    # qGen() Generates input rows for PSQL symbol table.
    # Accepts data.frame row as created in populating.R script.
    # Use with apply call from parent script.
    
    # input:
    #   fromYear - generate symbols from fromYear
    #   toYear - generate symbols to toYear
    
    c1 <- as.character(dfRowIn[1])
    c2 <- as.character(dfRowIn[2])
    c3 <- as.character(dfRowIn[3])
    c4 <- as.character(dfRowIn[4])
    c5 <- as.character(dfRowIn[5])
    c6 <- as.character(dfRowIn[6])
    c7 <- as.numeric(dfRowIn[7])
    
    if (fromYear < c7)
        input_years = twodigityears(c7, toYear)
    else
        input_years = twodigityears(fromYear, toYear)
    input_months <- strsplit(c2,",")
        
    lm <- str_count(input_months,"[A-Z]")
    ly <- length(input_years)
    queryRows <- vector("list",lm*ly)
    n <- 0
    for (y in 1:ly)
        for (m in 1:lm) {
            n <- n+1
            queryRows[[n]] = paste("('",c3,"','",c1,input_months[[1]][m],
                                   input_years[y],"','",c4,"','",c5,"','",
                                   c6,"'",",DEFAULT",",DEFAULT","),",sep = "")
        }
    return(queryRows)
}