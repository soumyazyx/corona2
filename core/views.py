import json
import csv
import requests
import pandas as pd
from datetime import datetime, timezone
from django.db.models import Sum
from django.shortcuts import render
from django.core import serializers
from django.http import HttpResponse, JsonResponse, response
# Custom imports
from core.models import Record, Summary


def home(request):

    model_values = Record.objects.all().filter(stats_type='confirmed').values('latitude','longitude','country_region')    
    summary_feed = requests.get('https://coronazyx.herokuapp.com/api/coronafeed')
    context = {
        "data": list(model_values),
        "summary": summary_feed.json()
    }
    return render(request, "index.html", context)


def sync(request):
    print("---------------------------------------------------")
    print("{}:Syncing records from web..".format(datetime.now()))
    
    # Truncate the table
    print("{}:Truncating [RECORD] table..".format(datetime.now()))
    Record.objects.all().delete()
    print("{}:Truncating [RECORD] table..Done".format(datetime.now()))
    
    # Recovered 
    recovered_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv'
    populateDb(stats_type='recovered', url=recovered_url)
    
    # Deaths
    deaths_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv'
    populateDb(stats_type='deaths', url=deaths_url)

    # Confirmed
    confirmed_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv'
    populateDb(stats_type='confirmed', url=confirmed_url)

    # Update summary table
    summary = updateSummaryTable()
    print("---------------------------------------------------")
    return JsonResponse(summary)


def populateDb(url, stats_type):
    
    print("{}:Fetching content for stats_type [{}]..".format(datetime.now(), stats_type))
    print("{}:URL in use [{}]".format(datetime.now(), url))
    with requests.Session() as s:
        download = s.get(url)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    print("{}:Fetching content for stats_type [{}]..Done".format(datetime.now(), stats_type))
    my_list = list(cr)
    header_row = my_list.pop(0)  # Header is the first row.
    header_row.pop(0)  # Remove the value 'Province/State'
    header_row.pop(0)  # Remove the value 'Country/Region'
    header_row.pop(0)  # Remove the value 'Lat'
    header_row.pop(0)  # Remove the value 'Long'
    latest_stats_date = header_row[-1]
    stats_dates_csv   = ",".join(header_row)    

    print("{}:Creating objects..".format(datetime.now()))
    objects_list = []
    for row in my_list[:]:
        state_province = row.pop(0)
        country_region = row.pop(0)
        latitude = row.pop(0)
        longitude = row.pop(0)
        stats_type = stats_type
        stats_value_csv = ",".join(row)
        latest_stats_value = row[-1]
        # Create model Record instances
        obj = Record(
            state_province     = state_province,
            country_region     = country_region,
            latitude           = latitude,
            longitude          = longitude,
            stats_type         = stats_type,
            latest_stats_date  = latest_stats_date,
            latest_stats_value = latest_stats_value,
            stats_dates_csv    = stats_dates_csv,
            stats_value_csv    = stats_value_csv,
        )
        objects_list.append(obj)        
    print("{}:Creating objects..Done".format(datetime.now()))
    print("{}:Total objects created = {}".format(datetime.now(), len(objects_list)))

    print("{}:Inserting records for stats_type[{}]..".format(datetime.now(), stats_type))
    Record.objects.bulk_create(objects_list)
    print("{}:Inserting records for stats_type[{}]..Done".format(datetime.now(), stats_type))


def updateSummaryTable():
    
    
    print("{}: Computing summary from records fetched..".format(datetime.now()))
    details = {}
    details['utc_dt'] = str(datetime.now(timezone.utc))
    details['totals'] = findSumAcrossAllCountries()['totals']
    details['countries'] = findSumAcrossEachCountry()['countries']
    details['countriesSorted_Deaths']    = findCountriesSorted(stats_type='deaths')
    details['countriesSorted_Recovered'] = findCountriesSorted(stats_type='recovered')
    details['countriesSorted_Confirmed'] = findCountriesSorted(stats_type='confirmed')
    print("{}: Computing summary from records fetched..Done".format(datetime.now()))

    # Truncate summary table
    print("{}:Truncating summary table..".format(datetime.now()))
    Summary.objects.all().delete()
    print("{}:Truncating summary table..Done".format(datetime.now()))
    # Update Summary table
    print("{}:Updating summary table..".format(datetime.now()))
    obj = Summary(json_string=json.dumps(details))
    obj.save()
    print("{}:Updating summary table..Done".format(datetime.now()))
    return details


def findSumAcrossAllCountries():
    # Find totals of confirmed/deaths/recovered across ALL countires
    temp = {}
    temp['totals'] = {}
    deaths_total = Record.objects.filter(stats_type='deaths').aggregate(Sum('latest_stats_value'))
    confirmed_total = Record.objects.filter(stats_type='confirmed').aggregate(Sum('latest_stats_value'))
    recovered_total = Record.objects.filter(stats_type='recovered').aggregate(Sum('latest_stats_value'))
    temp['totals']['total_deaths']    = deaths_total['latest_stats_value__sum']
    temp['totals']['total_confirmed'] = confirmed_total['latest_stats_value__sum']
    temp['totals']['total_recovered'] = recovered_total['latest_stats_value__sum']
    return temp


def findSumAcrossEachCountry():
    temp = {}
    temp['countries'] = {}
    query = """
    SELECT
        1 AS ID,
        COUNTRY_REGION,
        STATS_TYPE,
        SUM(LATEST_STATS_VALUE) AS TOTAL
    FROM
        PUBLIC.CORE_RECORD
    GROUP BY
        STATS_TYPE,
        COUNTRY_REGION
    ORDER BY
        COUNTRY_REGION,
        STATS_TYPE"""
    querySet = Record.objects.raw(query)
    for rec in querySet:
        if (not(rec.country_region in temp['countries'])):
            temp['countries'][rec.country_region] = {}
        temp['countries'][rec.country_region][rec.stats_type] = rec.total
    return temp


def findCountriesSorted(stats_type):
    lst = []
    sql = "SELECT 1 as ID, COUNTRY_REGION, SUM(LATEST_STATS_VALUE) AS TOTAL FROM PUBLIC.CORE_RECORD WHERE STATS_TYPE='{}' GROUP BY COUNTRY_REGION ORDER BY TOTAL DESC".format(stats_type)
    qs = Record.objects.raw(sql)
    for p in qs:
        lst.append(p.country_region)
    return lst
