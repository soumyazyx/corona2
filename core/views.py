import json
import csv
import folium
from folium import plugins
from folium.features import GeoJson, GeoJsonTooltip
# from branca.colormap import linear
import branca.colormap as cm
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


def home_20200329(request):

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
    summary_json = summary_feed.json()
    geo_json_data = json.loads(requests.get('https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/world-countries.json').text)
    for obj in geo_json_data['features']:
        geo_json_country = obj['properties']['name']
        summary_json_country = ''

        if (geo_json_country == 'United States of America'):
            summary_json_country = 'US'
        else:
            summary_json_country = geo_json_country

        try:
            obj['properties']['confirmed'] = summary_json['countries'][summary_json_country]['confirmed']
            obj['properties']['recovered'] = summary_json['countries'][summary_json_country]['recovered']
            obj['properties']['deaths']    = summary_json['countries'][summary_json_country]['deaths']
        except:
            pass
            # print(">>>{}".format(country)) # THIS SHOWS THE CNTRIES WHICH ARE NOT IN DUMMRY JSON
            # CHECK FOR CGTRIES WHICH ARE IN SUMMARY_JSON BUT NOT IN OBJ
    # print(geo_json_data)
    countries_df = get_country_dataframes()
    # print(countries_df)
    
    beep = "Dummy|0"
    for country in summary_json['countries']:
        deaths    = 0
        confirmed = 0
        recovered = 0
        if ('deaths' in summary_json['countries'][country]):
            deaths = summary_json['countries'][country]['deaths']
        if ('confirmed' in summary_json['countries'][country]):
            confirmed = summary_json['countries'][country]['confirmed']
        if ('recovered' in summary_json['countries'][country]):
            recovered = summary_json['countries'][country]['recovered']
        try:
            beep = beep + "\n" + "{}|{}".format(countries_df.loc[country,'alpha3'], confirmed)
        except:
            beep = beep + "\n" + "{}|{}".format(country, confirmed)
            print(country)

    TESTDATA = StringIO(beep)
    unemployment_df = pd.read_csv(
        TESTDATA,
        sep="|",
        names=["State","Unemployment"]
    )
    # print(unemployment_df)
    # print(colormap.linear.OrRd_09)
    # colormap = linear.OrRd_09.scale(
    #     unemployment_df.Unemployment.min(),
    #     unemployment_df.Unemployment.max()
    # )
    linearrrr = cm.LinearColormap(
        ['#fac4c4','#f8302e'],
        vmin=unemployment_df.Unemployment.min(), 
        vmax=unemployment_df.Unemployment.max()
    )
    
    unemployment_dict = unemployment_df.set_index('State')['Unemployment']
    color_dict = {key: linearrrr(unemployment_dict[key]) for key in unemployment_dict.keys()}
    # print(color_dict)
    color_dict['USA'] = color_dict['US']
    m = folium.Map([20.5937, 78.9629], zoom_start=1) 
    # tooltip=GeoJsonTooltip(
    #     fields=["name"],
    #     aliases=["name"],
    #     localize=True,
    #     sticky=False,
    #     labels=True,
    #     style="""
    #         background-color: #F0EFEF;
    #         border: 2px solid black;
    #         border-radius: 3px;
    #         box-shadow: 3px;
    #     """,
    # )
    # print(color_dict.keys())
    folium.GeoJson(
        geo_json_data,
        style_function=lambda feature: {
            'fillColor':color_dict[feature['id']] if feature['id'] in color_dict.keys() else '#262626',
            'color': 'white',
            'weight': 0.3,
            # 'dashArray': '5, 5',
            'fillOpacity': 0.9,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=['name','confirmed','recovered','deaths'],
            aliases=['Country','Confirmed', 'Recovered', 'Deaths'],
            localize=True
        )
    ).add_to(m)

    # folium.LayerControl().add_to(m)



    map_html = m.get_root().render()
    context = {
        'map_html': map_html,
        "data": list(model_values),
        "summary": summary_feed.json()
    }
    return render(request, "index-a.html", context)

def home(request):

    model_values = Record.objects.all().filter(stats_type='deaths').values(
        'latitude',
        'longitude',
        'country_region',
        'latest_stats_value'
    )

    summary_feed = requests.get('https://coronazyx.herokuapp.com/api/coronafeed')
    summary_json = summary_feed.json()
    geo_json_data = json.loads(requests.get('https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/world-countries.json').text)
    for obj in geo_json_data['features']:
        geo_json_country = obj['properties']['name']
        summary_json_country = ''

        if (geo_json_country == 'United States of America'):
            summary_json_country = 'US'
        else:
            summary_json_country = geo_json_country

        try:
            obj['properties']['confirmed'] = summary_json['countries'][summary_json_country]['confirmed']
            obj['properties']['recovered'] = summary_json['countries'][summary_json_country]['recovered']
            obj['properties']['deaths']    = summary_json['countries'][summary_json_country]['deaths']
        except:
            pass
            # print(">>>{}".format(country)) # THIS SHOWS THE CNTRIES WHICH ARE NOT IN DUMMRY JSON
            # CHECK FOR CGTRIES WHICH ARE IN SUMMARY_JSON BUT NOT IN OBJ
    # print(geo_json_data)
    countries_df = get_country_dataframes()
    # print(countries_df)
    
    beep = "Dummy|0"
    for country in summary_json['countries']:
        deaths    = 0
        confirmed = 0
        recovered = 0
        if ('deaths' in summary_json['countries'][country]):
            deaths = summary_json['countries'][country]['deaths']
        if ('confirmed' in summary_json['countries'][country]):
            confirmed = summary_json['countries'][country]['confirmed']
        if ('recovered' in summary_json['countries'][country]):
            recovered = summary_json['countries'][country]['recovered']
        try:
            beep = beep + "\n" + "{}|{}".format(countries_df.loc[country,'alpha3'], confirmed)
        except:
            beep = beep + "\n" + "{}|{}".format(country, confirmed)
            print(country)

    TESTDATA = StringIO(beep)
    unemployment_df = pd.read_csv(
        TESTDATA,
        sep="|",
        names=["State","Unemployment"]
    )
    # print(unemployment_df)
    # print(colormap.linear.OrRd_09)
    # colormap = linear.OrRd_09.scale(
    #     unemployment_df.Unemployment.min(),
    #     unemployment_df.Unemployment.max()
    # )
    linearrrr = cm.LinearColormap(
        ['#fac4c4','#f8302e'],
        vmin=unemployment_df.Unemployment.min(), 
        vmax=unemployment_df.Unemployment.max()
    )
    
    unemployment_dict = unemployment_df.set_index('State')['Unemployment']
    color_dict = {key: linearrrr(unemployment_dict[key]) for key in unemployment_dict.keys()}
    # print(color_dict)
    color_dict['USA'] = color_dict['US']
    m = folium.Map([20.5937, 78.9629], zoom_start=1) 
    # tooltip=GeoJsonTooltip(
    #     fields=["name"],
    #     aliases=["name"],
    #     localize=True,
    #     sticky=False,
    #     labels=True,
    #     style="""
    #         background-color: #F0EFEF;
    #         border: 2px solid black;
    #         border-radius: 3px;
    #         box-shadow: 3px;
    #     """,
    # )
    # print(color_dict.keys())
    folium.GeoJson(
        geo_json_data,
        style_function=lambda feature: {
            'fillColor':color_dict[feature['id']] if feature['id'] in color_dict.keys() else '#262626',
            'color': 'white',
            'weight': 0.3,
            # 'dashArray': '5, 5',
            'fillOpacity': 0.9,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=['name','confirmed','recovered','deaths'],
            aliases=['Country','Confirmed', 'Recovered', 'Deaths'],
            localize=True
        )
    ).add_to(m)

    # folium.LayerControl().add_to(m)



    map_html = m.get_root().render()
    context = {
        'map_html': map_html,
        "data": list(model_values),
        "summary": summary_feed.json()
    }
    return render(request, "index.html", context)


def get_country_dataframes( ):

    TESTDATA = StringIO("""
Afghanistan|AFG
Albania|ALB
Algeria|DZA
American Samoa|ASM
Andorra|AND
Angola|AGO
Anguilla|AIA
Antarctica|ATA
Antigua and Barbuda|ATG
Argentina|ARG
Armenia|ARM
Aruba|ABW
Australia|AUS
Austria|AUT
Azerbaijan|AZE
Bahamas|BHS
Bahrain|BHR
Bangladesh|BGD
Barbados|BRB
Belarus|BLR
Belgium|BEL
Belize|BLZ
Benin|BEN
Bermuda|BMU
Bhutan|BTN
Bolivia|BOL
Bonaire, Sint Eustatius and Saba|BES
Bosnia and Herzegovina|BIH
Botswana|BWA
Bouvet Island|BVT
Brazil|BRA
British Indian Ocean Territory (the)|IOT
Brunei|BRN
Bulgaria|BGR
Burkina Faso|BFA
Burundi|BDI
Cabo Verde|CPV
Cambodia|KHM
Cameroon|CMR
Canada|CAN
Cayman Islands (the)|CYM
Central African Republic|CAF
Chad|TCD
Chile|CHL
China|CHN
Christmas Island|CXR
Cocos (Keeling) Islands (the)|CCK
Colombia|COL
Comoros (the)|COM
Congo (Kinshasa)|COD
Congo (Brazzaville)|COG
Cook Islands (the)|COK
Costa Rica|CRI
Croatia|HRV
Cuba|CUB
Curaçao|CUW
Cyprus|CYP
Czechia|CZE
Cote d'Ivoire|CIV
Denmark|DNK
Djibouti|DJI
Dominica|DMA
Dominican Republic|DOM
Ecuador|ECU
Egypt|EGY
El Salvador|SLV
Equatorial Guinea|GNQ
Eritrea|ERI
Estonia|EST
Eswatini|SWZ
Ethiopia|ETH
Falkland Islands (the) [Malvinas]|FLK
Faroe Islands (the)|FRO
Fiji|FJI
Finland|FIN
France|FRA
French Guiana|GUF
French Polynesia|PYF
French Southern Territories (the)|ATF
Gabon|GAB
Gambia|GMB
Georgia|GEO
Germany|DEU
Ghana|GHA
Gibraltar|GIB
Greece|GRC
Greenland|GRL
Grenada|GRD
Guadeloupe|GLP
Guam|GUM
Guatemala|GTM
Guernsey|GGY
Guinea|GIN
Guinea-Bissau|GNB
Guyana|GUY
Haiti|HTI
Heard Island and McDonald Islands|HMD
Holy See|VAT
Honduras|HND
Hong Kong|HKG
Hungary|HUN
Iceland|ISL
India|IND
Indonesia|IDN
Iran|IRN
Iraq|IRQ
Ireland|IRL
Isle of Man|IMN
Israel|ISR
Italy|ITA
Jamaica|JAM
Japan|JPN
Jersey|JEY
Jordan|JOR
Kazakhstan|KAZ
Kenya|KEN
Kiribati|KIR
Korea, North|PRK
Korea, South|KOR
Kuwait|KWT
Kyrgyzstan|KGZ
Laos|LAO
Latvia|LVA
Lebanon|LBN
Lesotho|LSO
Liberia|LBR
Libya|LBY
Liechtenstein|LIE
Lithuania|LTU
Luxembourg|LUX
Macao|MAC
Madagascar|MDG
Malawi|MWI
Malaysia|MYS
Maldives|MDV
Mali|MLI
Malta|MLT
Marshall Islands (the)|MHL
Martinique|MTQ
Mauritania|MRT
Mauritius|MUS
Mayotte|MYT
Mexico|MEX
Micronesia (Federated States of)|FSM
Moldova|MDA
Monaco|MCO
Mongolia|MNG
Montenegro|MNE
Montserrat|MSR
Morocco|MAR
Mozambique|MOZ
Myanmar|MMR
Burma|MMR
Namibia|NAM
Nauru|NRU
Nepal|NPL
Netherlands|NLD
New Caledonia|NCL
New Zealand|NZL
Nicaragua|NIC
Niger|NER
Nigeria|NGA
Niue|NIU
Norfolk Island|NFK
Northern Mariana Islands (the)|MNP
Norway|NOR
Oman|OMN
Pakistan|PAK
Palau|PLW
Palestine, State of|PSE
Panama|PAN
Papua New Guinea|PNG
Paraguay|PRY
Peru|PER
Philippines|PHL
Pitcairn|PCN
Poland|POL
Portugal|PRT
Puerto Rico|PRI
Qatar|QAT
North Macedonia|MKD
Romania|ROU
Russia|RUS
Rwanda|RWA
Réunion|REU
Saint Barthélemy|BLM
Saint Helena, Ascension and Tristan da Cunha|SHN
Saint Kitts and Nevis|KNA
Saint Lucia|LCA
Saint Martin (French part)|MAF
Saint Pierre and Miquelon|SPM
Saint Vincent and the Grenadines|VCT
Samoa|WSM
San Marino|SMR
Sao Tome and Principe|STP
Saudi Arabia|SAU
Senegal|SEN
Serbia|SRB
Seychelles|SYC
Sierra Leone|SLE
Singapore|SGP
Sint Maarten (Dutch part)|SXM
Slovakia|SVK
Slovenia|SVN
Solomon Islands|SLB
Somalia|SOM
South Africa|ZAF
South Georgia and the South Sandwich Islands|SGS
South Sudan|SSD
Spain|ESP
Sri Lanka|LKA
Sudan|SDN
Suriname|SUR
Svalbard and Jan Mayen|SJM
Sweden|SWE
Switzerland|CHE
Syria|SYR
Taiwan*|TWN
Tajikistan|TJK
Tanzania|TZA
Thailand|THA
Timor-Leste|TLS
Togo|TGO
Tokelau|TKL
Tonga|TON
Trinidad and Tobago|TTO
Tunisia|TUN
Turkey|TUR
Turkmenistan|TKM
Turks and Caicos Islands (the)|TCA
Tuvalu|TUV
Uganda|UGA
Ukraine|UKR
United Arab Emirates|ARE
United Kingdom|GBR
United States Minor Outlying Islands (the)|UMI
United States of America|USA
Uruguay|URY
Uzbekistan|UZB
Vanuatu|VUT
Venezuela|VEN
Vietnam|VNM
Virgin Islands (British)|VGB
Virgin Islands (U.S.)|VIR
Wallis and Futuna|WLF
Western Sahara|ESH
Yemen|YEM
Zambia|ZMB
Zimbabwe|ZWE
Åland Islands|ALA
""")
    countries_df = pd.read_csv(
        TESTDATA,
        sep="|",
        names=["Country","alpha3"]
    )
    countries_df.set_index('Country',inplace=True)
    return countries_df