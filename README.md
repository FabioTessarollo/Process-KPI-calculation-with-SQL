# Process-KPI-calculation-with-SQL
I was asked to calculate some KPIs from a tracking table of processes with minute precision, with the constraint of considering only the valid ranges (eg. working time) and not considering some other ranges (eg. system process).
My algorithm is considering every possible combination, by calculating intervals as, for example: 
 - from the start of process to the end of the valid range
 - from the start of the valid range to the end of the process
 - summing valid whole ranges between start and end of the process
 - removing not valid intervals

Another approach was to flag every minute of the calendar and summing True minutes, but this was not scaling friendly, as a row for each miunute would exists.
