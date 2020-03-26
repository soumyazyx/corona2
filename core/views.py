import json
import csv
import folium
from folium import plugins
from folium.features import GeoJson, GeoJsonTooltip
from branca.colormap import linear
import pandas as pd
import numpy as np
from io import StringIO
import requests
import logging
from datetime import datetime, timezone
from django.db.models import Sum
from django.shortcuts import render
from django.core import serializers
from django.http import HttpResponse, JsonResponse, response
# Custom imports
from core.models import Record, Summary


def home(request):

    model_values = Record.objects.all().filter(stats_type='deaths').values(
        'latitude',
        'longitude',
        'country_region',
        'latest_stats_value'
    )
    summary_feed = requests.get('https://coronazyx.herokuapp.com/api/coronafeed')
    context = {
        "data": list(model_values),
        "summary": summary_feed.json()
    }
    return render(request, "index.html", context)


def home_test_choropleth(request):

    model_values = Record.objects.all().filter(stats_type='deaths').values(
        'latitude',
        'longitude',
        'country_region',
        'latest_stats_value'
    )
    summary_feed = requests.get('https://coronazyx.herokuapp.com/api/coronafeed')

    # url = 'https://raw.githubusercontent.com/python-visualization/folium/master/examples/data'
    # state_geo = f'{url}/us-states.json'
    state_geo = 'https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/world-countries.json'
    
    # state_unemployment = f'{url}/US_Unemployment_Oct2012.csv'
    
    beep = "Pakistanzyx|7"
    beep = beep + "\n" + "Indiazyx|10"
    # print(beep)
    summary_json = summary_feed.json()

    # print(summary_json['countries'])
    for country in summary_json['countries']:
        # print(country)
        confirmed = 0
        if ('confirmed' in summary_json['countries'][country]):
            confirmed = summary_json['countries'][country]['confirmed']
            
        beep = beep + "\n" + "{}|{}".format(country, confirmed)

    # print(beep)
#     state_unemployment = StringIO('''
# State,Unemployment
# Pakistan,7
# Bangladesh,8
# India,10
# ''')
    state_unemployment = StringIO(beep)
    state_data = pd.read_csv(state_unemployment, sep="|", names=["State","Unemployment"])
    # print(state_data)
    
    m = folium.Map(tiles="")
    # plugins.LocateControl(auto_start=True).add_to(m)
    folium.raster_layers.TileLayer('OpenStreetMap').add_to(m)
    # folium.raster_layers.TileLayer('Stamen Terrain').add_to(m)
    # folium.raster_layers.TileLayer('Stamen Watercolor').add_to(m)
    # folium.raster_layers.TileLayer('CartoDB positron').add_to(m)
    # folium.raster_layers.TileLayer('CartoDB dark_matter').add_to(m)
    # folium.LayerControl().add_to(m)

    folium.Choropleth(
        geo_data=state_geo,        
        data=state_data,
        columns=['State', 'Unemployment'],
        key_on='feature.properties.name',
        fill_color='YlOrRd',
        fill_opacity=0.7,
        line_opacity=0.2,
        nan_fill_color='white',
        nan_fill_opacity=0,
        # legend_name='Unemployment Rate (%)',
        line_color='red',
        # control_scale=False,
    ).add_to(m)

    # folium.LayerControl().add_to(m)
    map_html = m.get_root().render()

    context = {
        'map_html': map_html,
        "data": list(model_values),
        "summary": summary_feed.json()
    }
    return render(request, "index-a.html", context)


def sync(request):

    print("{}:Syncing records from web..".format(datetime.now()))

    # Each time we sync(once a day), we truncate the table of all its record
    # and then load afresh
    print("{}:Truncating [RECORD] table..".format(datetime.now()))
    Record.objects.all().delete()
    print("{}:Truncating [RECORD] table..Done".format(datetime.now()))

    url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series'
    deaths_url    = f'{url}/time_series_covid19_deaths_global.csv'
    confirmed_url = f'{url}/time_series_covid19_confirmed_global.csv'
    recovered_url = f'{url}/time_series_covid19_recovered_global.csv'

    populateDb(stats_type='deaths',    url=deaths_url)
    populateDb(stats_type='confirmed', url=confirmed_url)
    populateDb(stats_type='recovered', url=recovered_url)  # Discontinued!

    # Populate India stats
    populateIndiaStats(url='https://api.rootnet.in/covid19-in/stats/daily')

    # Update summary table
    summary = updateSummaryTable()
    print("{}:Syncing records from web..".format(datetime.now()))
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
        latest_stats_value = row[-1] or 0
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


def populateIndiaStats(url):

    Record.objects.filter(country_region='India').delete()

    # Globals
    states_lat_long = {
        "India" : {"lat":20.5937, "long":78.9629},
        "Andhra Pradesh" : {"lat":15.9129, "long":79.7400},
        "Assam" : {"lat":26.244156, "long":92.537842},
        "Bihar" : {"lat":25.0961, "long":85.3131},
        "Chandigarh" : {"lat":30.7333, "long":76.7794},
        "Chhattisgarh" : {"lat":21.295132, "long":81.828232},
        "Delhi" : {"lat":28.7041, "long":77.1025},
        "Gujarat" : {"lat":22.309425, "long":72.136230},
        "Haryana" : {"lat":29.238478, "long":76.431885},
        "Himachal Pradesh" : {"lat":32.084206, "long":77.571167},
        "Jammu and Kashmir" : {"lat":33.7782, "long":76.5762},
        "Karnataka" : {"lat":15.317277, "long":75.713890},
        "Kerala" : {"lat":10.850516, "long":76.271080},
        "Ladakh" : {"lat":34.152588, "long":77.577049},
        "Madhya Pradesh" : {"lat":23.473324, "long":77.947998},
        "Maharashtra" : {"lat":19.601194, "long":75.552979},
        "Odisha" : {"lat":20.940920, "long":84.803467},
        "Puducherry" : {"lat":11.9416, "long":79.8083},
        "Punjab" : {"lat":31.1471, "long":75.3412},
        "Rajasthan" : {"lat":27.391277, "long":73.432617},
        "Tamil Nadu" : {"lat":11.127123, "long":78.656891},
        "Telangana": {"lat":17.123184, "long":79.208824},
        "Telengana": {"lat":17.123184, "long":79.208824},
        "Tripura" : {"lat":23.745127, "long":91.746826},
        "Uttar Pradesh" : {"lat":28.207609, "long":79.826660},
        "Uttarakhand" : {"lat":30.0668, "long":79.0193},
        "West Bengal" : {"lat":22.978624, "long":87.747803}
    }

    state_wise_stats = {}
    # Fetch JSON data from url
    r = requests.get(url)
    r_json = r.json()
    for data in r_json['data']:
        date = data['day']
        date = datetime.strptime(date, "%Y-%m-%d").strftime("%y/%m/%d")
        for regional in data['regional']:
            state = regional['loc']
            if(state in state_wise_stats.keys()):
                pass
            else:
                state_wise_stats[state] = {}
                state_wise_stats[state]['confirmed_csv'] = ''
                state_wise_stats[state]['recovered_csv'] = ''
                state_wise_stats[state]['deaths_csv'] = ''
                state_wise_stats[state]['dates_csv'] = ''

            # Handle the case where the state is not present in the states_lat_long dict
            if (state in states_lat_long.keys()):
                state_wise_stats[state]['lat'] = states_lat_long[state]['lat']
                state_wise_stats[state]['long'] = states_lat_long[state]['long']
            else:
                state_wise_stats[state]['lat'] = states_lat_long['India']['lat']
                state_wise_stats[state]['long'] = states_lat_long['India']['long']
            state_wise_stats[state]['recovered_csv']    = state_wise_stats[state]['recovered_csv'] + str(regional['discharged']) + ","
            state_wise_stats[state]['confirmed_csv']    = state_wise_stats[state]['confirmed_csv'] + str(regional['confirmedCasesIndian'] + regional['confirmedCasesForeign']) + ","
            state_wise_stats[state]['deaths_csv']       = state_wise_stats[state]['deaths_csv'] + str(regional['deaths']) + ","
            state_wise_stats[state]['dates_csv']        = state_wise_stats[state]['dates_csv'] + str(date) + ","
            state_wise_stats[state]['confirmed_latest'] = regional['confirmedCasesIndian'] + regional['confirmedCasesForeign']
            state_wise_stats[state]['recovered_latest'] = regional['discharged']
            state_wise_stats[state]['deaths_latest']    = regional['deaths']
            state_wise_stats[state]['date_latest']      = str(date)
    # At this time, we have collected all the data into the state_wise_stats dict
    # Next step - Load data onto database using bulk insert
    # Bulk insert requires array of objects to be created
    objects_list = []
    for state in state_wise_stats:
        # For each state, we create 3 objects - 1.Confirmed 2.Recovered 3.Deaths
        obj = Record(
            state_province     = state,
            country_region     = 'India',
            latitude           = state_wise_stats[state]['lat'],
            longitude          = state_wise_stats[state]['long'],
            stats_type         = 'confirmed',
            latest_stats_date  = state_wise_stats[state]['date_latest'],
            latest_stats_value = state_wise_stats[state]['confirmed_latest'],
            stats_dates_csv    = state_wise_stats[state]['dates_csv'].rstrip(','),
            stats_value_csv    = state_wise_stats[state]['confirmed_csv'].rstrip(','),
        )
        objects_list.append(obj)
        obj = Record(
            state_province     = state,
            country_region     = 'India',
            latitude           = state_wise_stats[state]['lat'],
            longitude          = state_wise_stats[state]['long'],
            stats_type         = 'deaths',
            latest_stats_date  = state_wise_stats[state]['date_latest'],
            latest_stats_value = state_wise_stats[state]['deaths_latest'],
            stats_dates_csv    = state_wise_stats[state]['dates_csv'].rstrip(','),
            stats_value_csv    = state_wise_stats[state]['deaths_csv'].rstrip(','),
        )
        objects_list.append(obj)
        obj = Record(
            latitude           = state_wise_stats[state]['lat'],
            longitude          = state_wise_stats[state]['long'],
            stats_type         = 'recovered',
            state_province     = state,
            country_region     = 'India',
            stats_dates_csv    = state_wise_stats[state]['dates_csv'].rstrip(','),
            stats_value_csv    = state_wise_stats[state]['recovered_csv'].rstrip(','),
            latest_stats_date  = state_wise_stats[state]['date_latest'],
            latest_stats_value = state_wise_stats[state]['recovered_latest']
        )
        objects_list.append(obj)

    print("{}:Inserting INDIA records..".format(datetime.now()))
    Record.objects.bulk_create(objects_list)
    print("{}:Inserting INDIA records..Done".format(datetime.now()))






def updateSummaryTable():

    print("{}: Computing summary from records fetched..".format(datetime.now()))
    details = {}
    details['utc_dt'] = str(datetime.now(timezone.utc))
    details['totals'] = findSumAcrossAllCountries()['totals']
    details['countries'] = findSumAcrossEachCountry()['countries']
    details['trend_deaths']    = findTrend(stats_type='deaths')
    details['trend_confirmed'] = findTrend(stats_type='confirmed')
    details['trend_recovered'] = findTrend(stats_type='recovered')
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


def findTrend(stats_type):
    # Find the trend by counting sum of deaths/confirmed/recovered
    # across ALL countries for EACH date
    records = Record.objects.all().filter(stats_type=stats_type).values('stats_value_csv')
    trend = []
    # summ = [0 for i in range(15)]
    for record in records:
        for index, value in enumerate(record['stats_value_csv'].split(",")):
            if(value):
                pass
            else:
                value = 0
            try:
                trend[index] = trend[index] + int(value)
            except IndexError:
                trend.append(0)
                trend[index] = trend[index] + int(value)

    return trend



def home_test(request):

    model_values = Record.objects.all().filter(stats_type='deaths').values(
        'latitude',
        'longitude',
        'country_region',
        'latest_stats_value'
    )
    summary_feed = requests.get('https://coronazyx.herokuapp.com/api/coronafeed')

    url = 'https://raw.githubusercontent.com/python-visualization/folium/master/examples/data'
    us_states = f'{url}/us-states.json'
    geo_json_data = json.loads(requests.get(us_states).text)

    TESTDATA = StringIO("""
State,Unemployment
AL,7.1
AK,6.8
AZ,8.1
AR,7.2
CA,10.1
CO,7.7
CT,8.4
DE,7.1
FL,8.2
GA,8.8
HI,5.4
ID,6.6
IL,8.8
IN,8.4
IA,5.1
KS,5.6
KY,8.1
LA,5.9
ME,7.2
MD,6.8
MA,6.7
MI,9.1
MN,5.6
MS,9.1
MO,6.7
MT,5.8
NE,3.9
NV,10.3
NH,5.7
NJ,9.6
NM,6.8
NY,8.4
NC,9.4
ND,3.2
OH,6.9
OK,5.2
OR,8.5
PA,8
RI,10.1
SC,8.8
SD,4.4
TN,7.8
TX,6.4
UT,5.5
VT,5
VA,5.8
WA,7.8
WV,7.5
WI,6.8
WY,5.1
""")
    # CSV_FILE_NAME = 'temp_file.csv'  # Consider creating temp file, look URL below
    # with open(CSV_FILE_NAME, 'w') as outfile:
    #     outfile.write(TESTDATA)
    # unemployment = pd.read_csv(CSV_FILE_NAME, sep=',')

    unemployment = pd.read_csv(TESTDATA)
    print(unemployment)
    colormap = linear.YlGn_09.scale(
    unemployment.Unemployment.min(),
    unemployment.Unemployment.max())
    unemployment_dict = unemployment.set_index('State')['Unemployment']
    color_dict = {key: colormap(unemployment_dict[key]) for key in unemployment_dict.keys()}
    m = folium.Map([43, -100], zoom_start=4)
    tooltip=GeoJsonTooltip(
        fields=["name"],
        aliases=["name"],
        localize=True,
        sticky=False,
        labels=True,
        style="""
            background-color: #F0EFEF;
            border: 2px solid black;
            border-radius: 3px;
            box-shadow: 3px;
        """,
    )

    folium.GeoJson(
        geo_json_data,
        style_function=lambda feature: {
            'fillColor': color_dict[feature['id']],
            'color': 'black',
            'weight': 1,
            'dashArray': '5, 5',
            'fillOpacity': 0.9,
        },
        tooltip=tooltip
    ).add_to(m)

    # folium.LayerControl().add_to(m)



    map_html = m.get_root().render()
    context = {
        'map_html': map_html,
        "data": list(model_values),
        "summary": summary_feed.json()
    }
    return render(request, "index-a.html", context)
