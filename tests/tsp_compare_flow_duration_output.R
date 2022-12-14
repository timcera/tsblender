myargs <- commandArgs(trailingOnly = TRUE)

# this is where CMake BUILDS the TSPROC executable
bin_dir <- myargs[1]

tsproc_exe <- myargs[1]
tsproc_inp <- "tsproc_test_flow_duration.inp"
tsproc_log <- "tsproc_test_flow_duration.log"

read_ssf <- function(filename) {

  v<-list()

  v$info<-paste("Values imported from",filename)
  v$header<-c("Site","Date","Time","Value")

  dat<-read.table(filename,colClasses=c("character","character",
                                     "character","numeric"), header=F)
  colnames(dat)<-c("site","date","time","value")

  dat$date <- strptime(dat$date, format="%m/%d/%y")
  dat$datetime <- format(paste(dat$date, dat$time, sep=" "),format="%m/%d/%Y %H:%M%:%S")
  dat$julday <- as.numeric(dat$date)
  v$dat<-dat

  return(v)

}

mydata <- read_ssf("Q_BEC_BE_6500.ssf")

my_control_file <- c(
  "START SETTINGS",
  "  CONTEXT all",
  "  DATE_FORMAT mm/dd/yyyy",
  "END SETTINGS",

  "START GET_MUL_SERIES_SSF",
  "  CONTEXT all",
  "  FILE Q_BEC_BE_6500.ssf",
  "  SITE 05406500",
  "  NEW_SERIES_NAME Oq6500",
  "END GET_MUL_SERIES_SSF",

  "START FLOW_DURATION",
  "  CONTEXT all",
  "  SERIES_NAME Oq6500",
  "  NEW_TABLE_NAME flow_dur_obs",
  "END FLOW_DURATION",

  "START LIST_OUTPUT",
  "  CONTEXT all",
  "  FILE test__flow_duration.txt",
  "  G_TABLE_NAME flow_dur_obs",
  "END LIST_OUTPUT" )

writeLines(text=my_control_file,
           con=tsproc_inp)

retval <- system2(tsproc_exe, args=paste(tsproc_inp, tsproc_log, sep=" "))

tsp_result <- read.table("test__flow_duration.txt",
           skip=3,
           header=FALSE,
           sep=":")

colnames(tsp_result) <- c("Description", "value")

tempvec <- unlist(strsplit(x=as.character(tsp_result$Description),split="%"))
percentiles <- as.numeric(tempvec[grep("flows", tempvec, invert=TRUE)])


# Type 6
# ------
# m = p

# p[k] = k / (n + 1). Thus p[k] = E[F(x[k])]. This is used by Minitab and by SPSS.

tsp_values <- tsp_result$value
R_values <- quantile(mydata$dat$value, (100.-percentiles)/100., type=6 )

if( any((R_values - tsp_values) > 1e-4 ) ) {
  cat("FAIL")
} else {
  cat("PASS")
}
