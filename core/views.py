from django.conf import settings
import json
import csv
import numpy as np
import pickle
import requests
import logging
import threading
import time
from pathlib import Path
from datetime import datetime, timezone
from django.db.models import Sum
from django.shortcuts import render
from django.core import serializers
from django.http import HttpResponse, JsonResponse, response
# Custom imports
from core.models import Record, Summary
from lib.sync.sync_utils import sync_all, truncate_records, populate_records_world, populate_records_india, populate_summary_tbl_n_file, store_world_stats_table_html, store_world_choropleth_map_html, store_country_plotly_html, store_country_stats_table_html, populate_planetary_file
from lib.common.console import print_info
from lib.common.utils import get_country_dataframes


def sync(request):

    # sync_all is a long running process
    # Invoke via thread
    thread_sync = threading.Thread(target=sync_all); thread_sync.start()
    return HttpResponse("Sync initiated.Done")


def home(request):

    print_info("Processing starts..")

    print_info("Reading pickled data..")
    ConfirmedPickledFile = open('datasets/Confirmed.pickle', 'rb')
    confirmed_records = pickle.load(ConfirmedPickledFile)
    print_info("Reading pickled data..Done")

    print_info("Fetching summary..")
    summary_json = json.loads(open('datasets/summary.json').read())
    print_info("Fetching summary..Done")

    print_info("Fetching geo-json data..")
    geo_json_data = json.loads(open('datasets/GeoJsonWorldCountries.json').read())
    print_info("Fetching geo-json data..Done")

    print_info("Fetching HTML for counts table..")
    with open('datasets/html/world/world_stats_table.html') as file:
        table_html = file.read()
    print_info("Fetching HTML for counts table..Done")

    print_info("Fetching choropleth HTML from [datasets/html/world_choropleth.html]..")
    with open('datasets/html/world/world_choropleth.html') as file:
        choropleth_map_html = file.read()
    print_info("Fetching choropleth HTML from [datasets/html/world_choropleth.html]..Done")

    print_info("Setting context variable..")
    context = {
        "data": confirmed_records, # used for sparks
        "summary": summary_json, # used for pings
        'map_html': choropleth_map_html,
        'table_html': table_html
    }
    print_info("Setting context variable..Done")
    return render(request, "index.html", context)
