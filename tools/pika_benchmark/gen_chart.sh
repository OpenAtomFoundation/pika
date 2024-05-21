#!/bin/bash

mkdir -p $(pwd)/charts

/usr/bin/python3  gen_chart.py --filePath $(pwd)/parsed_data/cmd_ops --prefix=cmd --outPath=$(pwd)/charts
/usr/bin/python3  gen_chart.py --filePath $(pwd)/parsed_data/cmd_lantency --prefix=cmd --outPath=$(pwd)/charts
/usr/bin/python3  gen_chart.py --filePath $(pwd)/parsed_data/rw_ops --prefix=rw --outPath=$(pwd)/charts
/usr/bin/python3  gen_chart.py --filePath $(pwd)/parsed_data/rw_lantency --prefix=rw --outPath=$(pwd)/charts
