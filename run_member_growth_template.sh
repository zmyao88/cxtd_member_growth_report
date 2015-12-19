#!/bin/bash
MYDIRECTORY="/home/zaiming/work/xintiandi/project/reports/member_growth_report/"

# R CMD BATCH ${MYDIRECTORY}/monthly_excel_report.R /dev/tty 
Rscript ${MYDIRECTORY}/member_growth.R
