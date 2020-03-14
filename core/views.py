from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
# from csv import reader
import csv
import requests
import pandas as pd
from core.models import Record


def home(request):

    model_values = Record.objects.all().filter(stats_type='confirmed').exclude(country_region='US').values('latitude','longitude','country_region')
    context = {
        "data": list(model_values)
    }
    return render(request, "index.html", context)


def sync(request):

    # Truncate the table
    Record.objects.all().delete()
    # Recovered
    recovered_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv'
    populateDb(stats_type='recovered',  url=recovered_url)

    # Deaths
    deaths_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv'
    populateDb(stats_type='deaths',  url=deaths_url)

    # Confirmed
    confirmed_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv'
    populateDb(stats_type='confirmed',  url=confirmed_url)

    return HttpResponse('Sync complete!')    


def populateDb(url, stats_type):
    print("Inserting records for stats_type[{}]..".format(stats_type))
    with requests.Session() as s:
        download = s.get(url)

    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    header_row = my_list.pop(0)  # Header is the first row.
    header_row.pop(0)  # Remove the value 'Province/State'
    header_row.pop(0)  # Remove the value 'Country/Region'
    header_row.pop(0)  # Remove the value 'Lat'
    header_row.pop(0)  # Remove the value 'Long'
    latest_stats_date = header_row[-1]
    stats_dates_csv   = ",".join(header_row)

    for row in my_list[:]:
        state_province = row.pop(0)
        country_region = row.pop(0)
        latitude = row.pop(0)
        longitude = row.pop(0)
        stats_type = stats_type
        stats_value_csv = ",".join(row)
        latest_stats_value = row[-1]
        
        obj, created = Record.objects.get_or_create(
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
        print("country_region:{} latestdate:{} latestvalue:{}".format(country_region, latest_stats_date, latest_stats_value))

    print("Inserting records for stats_type[{}]..Done".format(stats_type))


def coronafeed(request):

    # url = "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
    # url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv"
    # c = pd.read_csv(url)
    # print(c)
    # resultJSON = c.to_json(orient='records')
    # print(resultJSON)
    # return JsonResponse(resultJSON, safe=False)

    return HttpResponse('wow')
