#!/bin/bash

# Base directory for TPC-DS
TPCDS_DIR=".."

# Directory where query templates are located
TEMPLATE_DIR="${TPCDS_DIR}/query_templates"

# Output directory for generated queries
OUTPUT_DIR="${TPCDS_DIR}/output"

# Make sure the output directory exists
mkdir -p $OUTPUT_DIR

# Loop to generate queries 1 through 99
for i in {1..99}
do
  # Generate the query number with leading zeroes
  QUERY_NUM=$i
  
  # Define input template file and output file
  INPUT_TEMPLATE="query${QUERY_NUM}.tpl"
  OUTPUT_FILE="${OUTPUT_DIR}/query${QUERY_NUM}.sql"
  
  # Run dsqgen for each query
  ${TPCDS_DIR}/tools/dsqgen -DIRECTORY ${TEMPLATE_DIR} -template ${INPUT_TEMPLATE} -OUTPUT_DIR ${OUTPUT_DIR} -SCALE 2 -DIALECT netezza -QUALIFY Y -VERBOSE Y
  
  cmd="mv $OUTPUT_DIR/query_0.sql $OUTPUT_FILE"
  echo "$cmd"; eval "$cmd"
  echo ""
done

echo "Generated 99 queries in ${OUTPUT_DIR}"