#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import json
import sys
import statistics
import numpy as np
import argparse
import re
import pygal
from pygal.style import Style
from itertools import chain
from os import walk
from os import path

chartStyle = Style(
    background='transparent',
    plot_background='transparent',
    font_family='googlefont:Montserrat',
    # colors=('#D8365D', '#78365D')
    colors=('#66CC69', '#173361', '#D8365D'),
    # colors=('#66CC69', '#667C69', '#173361', '#D8365D', '#78365D'),
)

#theme = pygal.style.CleanStyle
theme = chartStyle
fill = False

def create_quantile_chart(workload, title, y_label, time_series):
    import math
    chart = pygal.XY(style=theme, dots_size=0.5,
                     legend_at_bottom=True,
                     truncate_legend=37,
                     x_value_formatter=lambda x: '{:,.2f} %'.format(
                         100.0 - (100.0 / (10**x))),
                     show_dots=False, fill=fill,
                     stroke_style={'width': 2},
                     print_values=True, print_values_position='top',
                     show_y_guides=True, show_x_guides=False)
    chart.title = title
    # chart.stroke = False

    chart.human_readable = True
    chart.y_title = y_label
    chart.x_labels = [0.30103, 1, 2, 3]

    for label, values, opts in time_series:
        values = sorted((float(x), y) for x, y in values.items())
        xy_values = [(math.log10(100 / (100 - x)), y)
                     for x, y in values if x <= 99.9]
        chart.add(label, xy_values, stroke_style=opts)

    chart.render_to_file('%s/%s.svg' % (args.outPath, workload))

def create_bar_chart(workload, title, y_label, x_label, data):
    chart = pygal.Bar(
        style=theme, dots_size=1, show_dots=False, stroke_style={'width': 2}, fill=fill,
        show_legend=False, show_x_guides=False, show_y_guides=False,  print_values_position='center',
        print_values=True, show_y_labels=True, show_x_labels=True,
    )
    chart.title = title
    chart.x_labels = x_label
    chart.value_formatter = lambda y: "{:,.0f}".format(y)

    for label, points in data.items():
        chart.add(label, points)
    print ("workload", workload)
    chart.render_to_file('%s/%s.svg' % (args.outPath, workload))
#     chart.render_to_file('%s.svg' % workload)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Plot Kafka OpenMessaging Benchmark results')
    parser.add_argument('--filePath', dest='filePath', required=True, type=str,
                        help='Explicitly specify result files to plot')
    parser.add_argument('--prefix', dest='prefix', help='prefix of filename')
    parser.add_argument('--outPath', dest='outPath', help='out path to save the plot')
    args = parser.parse_args()

    aggregate = []

    for (dirpath, dirnames, filenames) in walk(args.filePath):
        for file in filenames:
            file_path = path.join(dirpath, file)
            data = json.load(open(file_path))
            data['file'] = file
            aggregate.append(data)

    opsPerSes = []
    latencyMap = []

    drivers = []

    pub_rate_avg = {}
    pub_rate_avg["Throughput (MB/s)"] = []

    colors = ['#2a6e3f', '#ee7959', '#ffee6f', '#e94829', '#667C69', '#173361', '#D8365D', '#33A1C9', '#e47690', '#FF5733', '#fac03d', "#f091a0"]

    # Aggregate across all runs
    count = 0
    for data in aggregate:

        if ('opsPerSes' in data and data['opsPerSes'] is not None):
            opsPerSes.append(data['opsPerSes'])

        if ('latencyMap' in data and data['latencyMap'] is not None):
            latencyMap.append(data['latencyMap'])

        drivers.append(data['file'])

        if ('opsPerSes' in data and data['opsPerSes'] is not None):
            pub_rate_avg["Throughput (MB/s)"].append(
                {
                    'value': data['opsPerSes'],
                    'color': colors[count]
                })
            count = count + 1

    # Parse plot options
    opts = []
    for driver in drivers:
        opts.append({})

    # Generate publish rate bar-chart
    svg = f'pika-{args.prefix}-ops'
    print(pub_rate_avg)
    if ("Throughput (MB/s)" in pub_rate_avg and len(pub_rate_avg["Throughput (MB/s)"]) > 0):
        create_bar_chart(svg, 'Cmd Ops', 'Ops',
                     drivers, pub_rate_avg)

    if(len(latencyMap) > 0):
        time_series = zip(drivers, latencyMap, opts)
        svg = f'pika-{args.prefix}-latency-quantile'
        create_quantile_chart(svg, 'Latency Quantiles',
                              y_label='Latency (ms)',
                              time_series=time_series)

