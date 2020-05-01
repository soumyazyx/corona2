from django.conf import settings
import json
import csv
import numpy as np
import pickle
import requests
import logging
from pathlib import Path
from datetime import datetime, timezone
from django.db.models import Sum
from django.shortcuts import render
from django.core import serializers
from django.http import HttpResponse, JsonResponse, response
# Custom imports
from core.models import Record, Summary
from lib.sync.sync_utils import rectifyDateFormat, populateWorldRecords, populateIndiaRecords, findSumAcrossAllCountries, findSumAcrossEachCountry, findTrend, findCountriesSorted, updateSummaryTable, store_world_stats_table_html, store_world_choropleth_map_html, populateUSRecords
from lib.common.console import print_info
from lib.common.utils import get_country_dataframes


def sync(request):

    print_info("Syncing records from web..")
    # Each time we sync(once a day), we truncate the table of all its record and then load afresh
    print_info("Truncating [RECORD] table..")
    Record.objects.all().delete()
    print_info("Truncating [RECORD] table..Done")

    url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series'
    deaths_url = f'{url}/time_series_covid19_deaths_global.csv'
    confirmed_url = f'{url}/time_series_covid19_confirmed_global.csv'
    recovered_url = f'{url}/time_series_covid19_recovered_global.csv'

    # Fetch the mappings between country and alpha3.
    # It will be used to populate alpha3 column in RECORD table
    countries_df = get_country_dataframes()

    # Fetch the CSV from web
    # The CSV contains details of all the countries
    death_url_content = populateWorldRecords(
        stats_type='deaths',    url=deaths_url,    countries_df=countries_df)
    confirmed_url_content = populateWorldRecords(
        stats_type='confirmed', url=confirmed_url, countries_df=countries_df)
    recovered_url_content = populateWorldRecords(
        stats_type='recovered', url=recovered_url, countries_df=countries_df)

    # Populate India records
    # The CSV content as fetched from above URLs dont contain granular deatils for India
    # So, populating more granular details from the below India-specific url
    populateIndiaRecords(url='https://api.rootnet.in/covid19-in/stats/daily')

    # Populate India records
    # The CSV content as fetched from above URLs dont contain granular deatils for India
    # So, populating more granular details from the below India-specific url
    populateUSRecords(url='https://api.rootnet.in/covid19-in/stats/daily')

    # Update summary table
    summary = updateSummaryTable()
    print_info("Syncing records from web..Done")

    print_info("Writing summary to local file[summary.json]..")
    with open("datasets/summary.json", "w") as outfile:
        json.dump(summary, outfile)
    print_info("Writing summary to local file[summary.json]..Done")

    # Store formatted data in a local file - to be consumed by planetaryjs
    # Reading it real time from DB is very slow
    print_info("Storing pickled content to local file..")
    confirmed_records_qs = Record.objects.all().filter(stats_type='confirmed').values(
        'latitude',
        'longitude',
        'country_region',
        'latest_stats_value'
    )
    confirmed_records = list(confirmed_records_qs)
    ConfirmedPickledFile = open('datasets/Confirmed.pickle', 'ab')
    pickle.dump(confirmed_records, ConfirmedPickledFile)
    ConfirmedPickledFile.close()
    print_info("Storing pickled content to local file..Done")

    store_world_stats_table_html()
    store_world_choropleth_map_html()
    return JsonResponse(summary)


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
    geo_json_data = json.loads(
        open('datasets/GeoJsonWorldCountries.json').read())
    print_info("Fetching geo-json data..Done")

    print_info("Fetching HTML for counts table..")
    with open('datasets/html/world_stats_table.html') as file:
        table_html = file.read()
    print_info("Fetching HTML for counts table..Done")

    print_info(
        "Fetching choropleth HTML from [datasets/html/world_choropleth.html]..")
    with open('datasets/html/world_choropleth.html') as file:
        choropleth_map_html = file.read()
    print_info(
        "Fetching choropleth HTML from [datasets/html/world_choropleth.html]..Done")

    print_info("Setting context variable..")
    context = {
        "data": confirmed_records,  # used for sparks
        "summary": summary_json,  # used for pings
        'map_html': choropleth_map_html,
        'table_html': table_html
    }
    print_info("Setting context variable..Done")
    return render(request, "index.html", context)
