#!/bin/bash
set -e

go test ../... -v

cd test
python main.py