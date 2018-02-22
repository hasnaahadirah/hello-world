twodigityears <- function(born_year, end_year) {
    
    years <- as.character(seq(born_year,end_year))
    years2 <- str_sub(years,-2)
    return(years2) # character array
}