import json
import csv
import requests
from django.db.models import Sum
from datetime import datetime, timezone
# Custom imports
from core.models import Record,Summary
from lib.sync.sync_utils import rectifyDateFormat
from lib.common.console import print_info


def populateWorldRecords(url, stats_type, countries_df):

    print_info(f"Fetching content for stats_type [{stats_type}]..")
    print_info(f"URL in use [{url}]")
    with requests.Session() as s:
        download = s.get(url)
    decoded_content = download.content.decode('utf-8')

    rows_fetched = list(csv.reader(decoded_content.splitlines(), delimiter=','))
    print_info(f"Fetching content for stats_type [{stats_type}]..Done")

    # Handle header row
    header_row = rows_fetched.pop(0)  # Header is the first row.
    header_row.pop(0)  # Remove the value 'Province/State'
    header_row.pop(0)  # Remove the value 'Country/Region'
    header_row.pop(0)  # Remove the value 'Lat'
    header_row.pop(0)  # Remove the value 'Long'
    latest_stats_date = rectifyDateFormat(header_row[-1])
    # stats_dates_csv   = 
    stats_dates_csv   = rectifyDateFormat(dates_csv=(",".join(header_row)))

    print_info("Creating objects..")
    objects_list = []
    ignored_countries = [
        'Diamond Princess',
        'West Bank and Gaza',
        'Kosovo',
        'MS Zaandam'
    ]
    for row in rows_fetched[:]:
        state_province = row.pop(0)
        country_region = row.pop(0)
        if (country_region in ignored_countries):
            country_alpha3 = '---'
        else:
            country_alpha3 = countries_df.loc[country_region,'alpha3']
        latitude = row.pop(0)
        longitude = row.pop(0)
        stats_type = stats_type
        stats_value_csv = ",".join(row)
        latest_stats_value = row[-1] or 0
        # Create model Record instances
        obj = Record(
            state_province     = state_province,
            country_region     = country_region,
            country_alpha3     = country_alpha3,
            latitude           = latitude,
            longitude          = longitude,
            stats_type         = stats_type,
            latest_stats_date  = latest_stats_date,
            latest_stats_value = latest_stats_value,
            stats_dates_csv    = stats_dates_csv,
            stats_value_csv    = stats_value_csv,
        )
        objects_list.append(obj)

    print_info("Creating objects..Done")
    print_info(f"Total objects created = {len(objects_list)}")

    print_info(f"Inserting records for stats_type[{stats_type}]..")
    Record.objects.bulk_create(objects_list)
    print_info(f"Inserting records for stats_type[{stats_type}]..Done")
    
    return decoded_content


def populateIndiaRecords(url):

    print_info("Handling India records..")
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
        date = data['day'] # 2020-03-10
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
            country_alpha3     = 'IND',
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
            country_alpha3     = 'IND',
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
            country_alpha3     = 'IND',
            stats_dates_csv    = state_wise_stats[state]['dates_csv'].rstrip(','),
            stats_value_csv    = state_wise_stats[state]['recovered_csv'].rstrip(','),
            latest_stats_date  = state_wise_stats[state]['date_latest'],
            latest_stats_value = state_wise_stats[state]['recovered_latest']
        )
        objects_list.append(obj)

    print_info("Inserting INDIA records..")
    Record.objects.bulk_create(objects_list)
    print_info("Inserting INDIA records..Done")
    print_info("Handling India records..Done")


def updateSummaryTable():

    print_info("Computing summary from records fetched..")
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
    print_info("Computing summary from records fetched..Done")

    # Truncate summary table
    print_info("Truncating summary table..")
    Summary.objects.all().delete()
    print_info("Truncating summary table..Done")
    
    # Update Summary table
    print_info("Updating summary table..")
    obj = Summary(json_string=json.dumps(details))
    obj.save()
    print_info("Updating summary table..Done")
    
    return details


def findSumAcrossAllCountries():
    # Find totals of confirmed/deaths/recovered across ALL countires
    temp = {}
    temp['totals'] = {}
    deaths_total    = Record.objects.filter(stats_type='deaths').aggregate(Sum('latest_stats_value'))
    confirmed_total = Record.objects.filter(stats_type='confirmed').aggregate(Sum('latest_stats_value'))
    recovered_total = Record.objects.filter(stats_type='recovered').aggregate(Sum('latest_stats_value'))
    temp['totals']['total_deaths']    = deaths_total['latest_stats_value__sum']
    temp['totals']['total_confirmed'] = confirmed_total['latest_stats_value__sum']
    temp['totals']['total_recovered'] = recovered_total['latest_stats_value__sum']
    return temp


def findSumAcrossEachCountry():
    temp = {}
    temp['countries'] = {}
    query = "SELECT 1 AS ID,COUNTRY_ALPHA3,STATS_TYPE,SUM(LATEST_STATS_VALUE) AS TOTAL FROM PUBLIC.CORE_RECORD GROUP BY STATS_TYPE,COUNTRY_ALPHA3 ORDER BY COUNTRY_ALPHA3,STATS_TYPE"
    querySet = Record.objects.raw(query)
    for rec in querySet:
        if (not(rec.country_alpha3 in temp['countries'])):
            temp['countries'][rec.country_alpha3] = {}
        temp['countries'][rec.country_alpha3][rec.stats_type] = rec.total
    return temp


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


def findCountriesSorted(stats_type):
    lst = []
    sql = "SELECT 1 as ID, COUNTRY_ALPHA3, SUM(LATEST_STATS_VALUE) AS TOTAL FROM PUBLIC.CORE_RECORD WHERE STATS_TYPE='{}' GROUP BY COUNTRY_ALPHA3 ORDER BY TOTAL DESC".format(stats_type)
    qs  = Record.objects.raw(sql)
    for p in qs:
        lst.append(p.country_alpha3)
    return lst

