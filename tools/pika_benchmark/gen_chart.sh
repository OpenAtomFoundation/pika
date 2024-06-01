#!/bin/bash

mkdir -p $(pwd)/charts

python3  gen_chart.py --filePath $(pwd)/parsed_data/cmd_ops --prefix=cmd --outPath=$(pwd)/charts
python3  gen_chart.py --filePath $(pwd)/parsed_data/cmd_latency --prefix=cmd --outPath=$(pwd)/charts
python3  gen_chart.py --filePath $(pwd)/parsed_data/rw_ops --prefix=rw --outPath=$(pwd)/charts
python3  gen_chart.py --filePath $(pwd)/parsed_data/rw_latency --prefix=rw --outPath=$(pwd)/charts
