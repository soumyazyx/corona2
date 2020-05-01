import csv
import time
import json
import folium
import requests
import pickle
import threading
import pandas as pd
import branca.colormap as cm
from folium import plugins
from folium.features import GeoJson, GeoJsonTooltip
from io import StringIO
from pathlib import Path
from django.db.models import Sum
from datetime import datetime, timezone
# Custom imports
from core.models import Record,Summary
from lib.common.console import print_info, print_error, print_warn
from lib.common.utils import get_country_dataframes
import plotly
import plotly.graph_objs as go

def sync_all():

    truncate_records()              # The github data source refreshes in every 24hrs.
                                    # When we sync, we truncate & load afresh
    populate_records_world()        # Get content from github source and populate records
    populate_records_india()        # Get content from India specific source and update India records
    populate_summary_tbl_n_file()   # Once the RECORDS table is updated, populate the SUMMARY table

    # Once the RECORDS/SUMMARY tables are populated,
    # the below methods have no other dependancies
    # and are run via threads for performance improvement
    thread_planetary     = threading.Thread(target=populate_planetary_file); thread_planetary.start()
    thread_world_stats   = threading.Thread(target=store_world_stats_table_html); thread_world_stats.start()
    thread_country_stats = threading.Thread(target=store_country_stats_table_html); thread_country_stats.start()
    thread_choropleth    = threading.Thread(target=store_world_choropleth_map_html); thread_choropleth.start()
    thread_plotly        = threading.Thread(target=store_country_plotly_html); thread_plotly.start()


def rectifyDateFormat(dates_csv):
    import datetime
    dates_list = []
    for date in dates_csv.split(","):

        month, day, year = date.split("/")
        year = '20' + year
        x = datetime.datetime(int(year), int(month), int(day))
        x = str(x.date())
        dates_list.append(x)
    dates_csv = ",".join(dates_list)
    return dates_csv


def populate_records_world():

    base_url      = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series'
    deaths_url    = f'{base_url}/time_series_covid19_deaths_global.csv'
    confirmed_url = f'{base_url}/time_series_covid19_confirmed_global.csv'
    recovered_url = f'{base_url}/time_series_covid19_recovered_global.csv'

    # Fetch the mappings between country and alpha3.
    # It will be used to populate alpha3 column in RECORD table
    countries_df = get_country_dataframes()

    # Fetch the CSV from web
    # The CSV contains details of all the countries

    death_url_content     = populateWorldRecords(stats_type='deaths',    url=deaths_url,    countries_df=countries_df)
    confirmed_url_content = populateWorldRecords(stats_type='confirmed', url=confirmed_url, countries_df=countries_df)
    recovered_url_content = populateWorldRecords(stats_type='recovered', url=recovered_url, countries_df=countries_df)


def populateWorldRecords(url, stats_type, countries_df):

    print_info(f"Fetching content for stats_type [{stats_type}]..")
    print_info(f"URL in use [{url}]")
    with requests.Session() as s:
        download = s.get(url)
    decoded_content = download.content.decode('utf-8')

    # local_file_name = f'datasets/{stats_type}.csv'
    # print_info(f"Writing dowloaded content to file[{local_file_name}]..")
    # with open(local_file_name, "w") as outfile:
    #     outfile.write(decoded_content)
    # print_info("Writing summary to local file..Done")


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


def populate_records_india(url='https://api.rootnet.in/covid19-in/stats/daily'):

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


def populate_summary_tbl_n_file():

    print_info("Computing summary from records fetched..")
    summary = {}
    summary['utc_dt'] = str(datetime.now(timezone.utc))
    summary['totals'] = findSumAcrossAllCountries()['totals']
    summary['countries'] = findSumAcrossEachCountry()['countries']
    summary['trend_deaths']    = findTrend(stats_type='deaths')
    summary['trend_confirmed'] = findTrend(stats_type='confirmed')
    summary['trend_recovered'] = findTrend(stats_type='recovered')
    summary['countriesSorted_Deaths']    = findCountriesSorted(stats_type='deaths')
    summary['countriesSorted_Recovered'] = findCountriesSorted(stats_type='recovered')
    summary['countriesSorted_Confirmed'] = findCountriesSorted(stats_type='confirmed')
    print_info("Computing summary from records fetched..Done")

    # Truncate Summary table
    print_info("Truncating summary table..")
    Summary.objects.all().delete()
    print_info("Truncating summary table..Done")

    # Update Summary table
    print_info("Updating summary table..")
    obj = Summary(json_string=json.dumps(summary))
    obj.save()
    print_info("Updating summary table..Done")

    write_to_file_summary_json(summary=summary)


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


def store_world_stats_table_html():

    print_info("Generating HTML for world stats table..")

    summary_json = json.loads(open('datasets/summary.json').read())
    geo_json_data = json.loads(open('datasets/GeoJsonWorldCountries.json').read())

    # Create a local simplified dict from topojson - like below
    # cntry = { "AFG": "Afghanistan", "ALB": "Albania", "DZA": "Algeria", "AND": "Andorra", "AGO": "Angola", "ATG": "Antigua", "ARG": "Argentina", "ARM": "Armenia", "AUS": "Australia", "AUT": "Austria", "AZE": "Azerbaijan", "BHS": "Bahamas", "BHR": "Bahrain", "BGD": "Bangladesh", "BRB": "Barbados", "BLR": "Belarus", "BEL": "Belgium", "BLZ": "Belize", "BEN": "Benin", "BTN": "Bhutan", "BOL": "Bolivia", "BIH": "Bosnia", "BWA": "Botswana", "BRA": "Brazil", "BRN": "Brunei", "BGR": "Bulgaria", "BFA": "Burkina", "BDI": "Burundi", "CPV": "CaboVerde", "KHM": "Cambodia", "CMR": "Cameroon", "CAN": "Canada", "CAF": "Central African Republic", "TCD": "Chad", "CHL": "Chile", "CHN": "China", "COL": "Colombia", "COM": "Comoros", "COG": "Congo", "COD": "Congo", "CRI": "Costa Rica", "CIV": "Côte d'Ivoire", "HRV": "Croatia", "CUB": "Cuba", "CYP": "Cyprus", "CZE": "Czechia", "DNK": "Denmark", "DJI": "Djibouti", "DMA": "Dominica", "DOM": "Dominican Rep", "ECU": "Ecuador", "EGY": "Egypt", "SLV": "El Salvador", "GNQ": "Guinea", "ERI": "Eritrea", "EST": "Estonia", "SWZ": "Eswatini", "ETH": "Ethiopia", "FJI": "Fiji", "FIN": "Finland", "FRA": "France", "GAB": "Gabon", "GMB": "Gambia", "GEO": "Georgia", "DEU": "Germany", "GHA": "Ghana", "GRC": "Greece", "GRD": "Grenada", "GTM": "Guatemala", "GIN": "Guinea", "GNB": "Guinea Bissau", "GUY": "Guyana", "HTI": "Haiti", "HND": "Honduras", "HUN": "Hungary", "ISL": "Iceland", "IND": "India", "IDN": "Indonesia", "IRN": "Iran", "IRQ": "Iraq", "IRL": "Ireland", "ISR": "Israel", "ITA": "Italy", "JAM": "Jamaica", "JPN": "Japan", "JOR": "Jordan", "KAZ": "Kazakhstan", "KEN": "Kenya", "KIR": "Kiribati", "PRK": "S Korea", "KOR": "N Korea", "KWT": "Kuwait", "KGZ": "Kyrgyzstan", "LAO": "Lao", "LVA": "Latvia", "LBN": "Lebanon", "LSO": "Lesotho", "LBR": "Liberia", "LBY": "Libya", "LIE": "Liechten stein", "LTU": "Lithuania", "LUX": "Luxembourg", "MDG": "Madagascar", "MWI": "Malawi", "MYS": "Malaysia", "MDV": "Maldives", "MLI": "Mali", "MLT": "Malta", "MHL": "Marshall Islands", "MRT": "Mauritania", "MUS": "Mauritius", "MEX": "Mexico", "FSM": "Micronesia", "MDA": "Moldova", "MCO": "Monaco", "MNG": "Mongolia", "MNE": "Montenegro", "MAR": "Morocco", "MOZ": "Mozambique", "MMR": "Myanmar", "NAM": "Namibia", "NRU": "Nauru", "NPL": "Nepal", "NLD": "Nether lands", "NZL": "New Zealand", "NIC": "Nicaragua", "NER": "Niger", "NGA": "Nigeria", "MKD": "North Macedonia", "NOR": "Norway", "OMN": "Oman", "PAK": "Pakistan", "PLW": "Palau", "PAN": "Panama", "PNG": "Papua New Guinea", "PRY": "Paraguay", "PER": "Peru", "PHL": "Philippines", "POL": "Poland", "PRT": "Portugal", "QAT": "Qatar", "ROU": "Romania", "RUS": "Russian", "RWA": "Rwanda", "KNA": "Saint Kitts and Nevis", "LCA": "Saint Lucia", "VCT": "Saint Vincent and the Grenadines", "WSM": "Samoa", "SMR": "San Marino", "STP": "Sao Tome and Principe", "SAU": "Saudi Arabia", "SEN": "Senegal", "SRB": "Serbia", "SYC": "Seychelles", "SLE": "Sierra Leone", "SGP": "Singapore", "SVK": "Slovakia", "SVN": "Slovenia", "SLB": "Solomon", "SOM": "Somalia", "ZAF": "South Africa", "SSD": "South Sudan", "ESP": "Spain", "LKA": "Sri Lanka", "SDN": "Sudan", "SUR": "Suriname", "SWE": "Sweden", "CHE": "Switzer land", "SYR": "Syria", "TJK": "Tajikistan", "TZA": "Tanzania", "THA": "Thailand", "TLS": "Timor Leste", "TGO": "Togo", "TON": "Tonga", "TTO": "Trinidad and Tobago", "TUN": "Tunisia", "TUR": "Turkey", "TKM": "Turkmeni stan", "TUV": "Tuvalu", "UGA": "Uganda", "UKR": "Ukraine", "ARE": "UAE", "GBR": "United Kingdom", "USA": "USA", "URY": "Uruguay", "UZB": "Uzbekistan", "VUT": "Vanuatu", "VEN": "Venezuela", "VNM": "Viet Nam", "YEM": "Yemen", "ZMB": "Zambia", "ZWE": "Zimbabwe" }
    cntry = {}
    for temp in geo_json_data['features']:
        cntry[temp['id']] = temp['properties']['name']
    html = ''
    for alpha3 in summary_json['countriesSorted_Confirmed']:
        if alpha3 == '---':
            continue
        if alpha3 in cntry:
            country = cntry[alpha3]
        else:
            country = alpha3

        confirmed = summary_json['countries'][alpha3]['confirmed']
        recovered = summary_json['countries'][alpha3]['recovered']
        deaths    = summary_json['countries'][alpha3]['deaths']

        country_href = f"<a href='/country/{alpha3}'>{country}</a>"
        html += f"<tr>"
        html += f"<td>{country_href}</td>"
        html += f"<td class='text-right text-warning'>{confirmed}</td>"
        html += f"<td class='text-right text-success'>{recovered}</td>"
        html += f"<td class='text-right text-danger'>{deaths}</td>"
        html += f"</tr>"
        html += f"\n"

    table_html = f'\
        <table id="table-count" class="table table-sm" style="width: 100%;max-height:180px;overflow:auto;">\
            <thead>\
            <tr>\
                <td></td>\
                <td class="text-right text-warning">Confirmed</td>\
                <td class="text-right text-success">Recovered</td>\
                <td class="text-right text-danger">Deaths</td>\
            </tr>\
            </thead>\
            <tbody>{html}</tbody>\
        </table>'
    print_info("Generating HTML for world stats table..")

    print_info("Writing generated HTML in local file[datasets/html/world/world_stats_table.html]..")

    local_file_name = f'datasets/html/world/world_stats_table.html'
    Path("datasets/html/world").mkdir(parents=True, exist_ok=True)
    with open(local_file_name, "w") as outfile:
        outfile.write(table_html)
    print_info("Writing generated HTML in local file[datasets/html/world/world_stats_table.html]..Done")

    return table_html


def store_world_choropleth_map_html():

    print_info("Generating HTML for world choropleth map..")

    summary_json = json.loads(open('datasets/summary.json').read())
    geo_json_data = json.loads(open('datasets/GeoJsonWorldCountries.json').read())

    # We are directly manipulating geojson to add in confirmed/recovered/deaths
    # We need to manipulate the geojson as it is the one which choropleth consumes
    # Must be a better approach available - TBD
    # Manipulating geo-json data..
    for obj in geo_json_data['features']:
        country_alpha2 = obj['id']                 # AFG
        try:
            obj['properties']['confirmed'] = summary_json['countries'][country_alpha2]['confirmed']
            obj['properties']['recovered'] = summary_json['countries'][country_alpha2]['recovered']
            obj['properties']['deaths']    = summary_json['countries'][country_alpha2]['deaths']
        except:
            # These are the countries which are present in geojson but not in summary
            # which implies - these are the countries wherein covid has not been reported
            # No further action is needed
            pass
    # Manipulating geo-json data..Done

    countries_df = get_country_dataframes()

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

    TESTDATA = StringIO(beep)
    unemployment_df = pd.read_csv(
        TESTDATA,
        sep="|",
        names=["State", "Unemployment"]
    )

    linearrrr = cm.LinearColormap(
        ['#fac4c4', '#f8302e'],
        vmin=unemployment_df.Unemployment.min(),
        vmax=unemployment_df.Unemployment.max()
    )

    unemployment_dict = unemployment_df.set_index('State')['Unemployment']
    color_dict = {key: linearrrr(unemployment_dict[key]) for key in unemployment_dict.keys()}

    m = folium.Map()

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
    choropleth_map_html = m.get_root().render()
    print_info("Generating HTML for world choropleth map..Done")

    print_info("Writing generated HTML in local file[datasets/html/world_choropleth.html]..")
    local_file_name = f'datasets/html/world/world_choropleth.html'
    Path("datasets/html/world").mkdir(parents=True, exist_ok=True)
    with open(local_file_name, "w") as outfile:
        outfile.write(choropleth_map_html)
    print_info("Writing generated HTML in local file[datasets/html/world/world_choropleth.html]..Done")


def store_country_plotly_html():

    summary_json = json.loads(open('datasets/summary.json').read())
    geo_json_data = json.loads(open('datasets/GeoJsonWorldCountries.json').read())

    # Create a local simplified dict from topojson - like below
    # cntry = { "AFG": "Afghanistan", "ALB": "Albania", "DZA": "Algeria", "AND": "Andorra", "AGO": "Angola", "ATG": "Antigua", "ARG": "Argentina", "ARM": "Armenia", "AUS": "Australia", "AUT": "Austria", "AZE": "Azerbaijan", "BHS": "Bahamas", "BHR": "Bahrain", "BGD": "Bangladesh", "BRB": "Barbados", "BLR": "Belarus", "BEL": "Belgium", "BLZ": "Belize", "BEN": "Benin", "BTN": "Bhutan", "BOL": "Bolivia", "BIH": "Bosnia", "BWA": "Botswana", "BRA": "Brazil", "BRN": "Brunei", "BGR": "Bulgaria", "BFA": "Burkina", "BDI": "Burundi", "CPV": "CaboVerde", "KHM": "Cambodia", "CMR": "Cameroon", "CAN": "Canada", "CAF": "Central African Republic", "TCD": "Chad", "CHL": "Chile", "CHN": "China", "COL": "Colombia", "COM": "Comoros", "COG": "Congo", "COD": "Congo", "CRI": "Costa Rica", "CIV": "Côte d'Ivoire", "HRV": "Croatia", "CUB": "Cuba", "CYP": "Cyprus", "CZE": "Czechia", "DNK": "Denmark", "DJI": "Djibouti", "DMA": "Dominica", "DOM": "Dominican Rep", "ECU": "Ecuador", "EGY": "Egypt", "SLV": "El Salvador", "GNQ": "Guinea", "ERI": "Eritrea", "EST": "Estonia", "SWZ": "Eswatini", "ETH": "Ethiopia", "FJI": "Fiji", "FIN": "Finland", "FRA": "France", "GAB": "Gabon", "GMB": "Gambia", "GEO": "Georgia", "DEU": "Germany", "GHA": "Ghana", "GRC": "Greece", "GRD": "Grenada", "GTM": "Guatemala", "GIN": "Guinea", "GNB": "Guinea Bissau", "GUY": "Guyana", "HTI": "Haiti", "HND": "Honduras", "HUN": "Hungary", "ISL": "Iceland", "IND": "India", "IDN": "Indonesia", "IRN": "Iran", "IRQ": "Iraq", "IRL": "Ireland", "ISR": "Israel", "ITA": "Italy", "JAM": "Jamaica", "JPN": "Japan", "JOR": "Jordan", "KAZ": "Kazakhstan", "KEN": "Kenya", "KIR": "Kiribati", "PRK": "S Korea", "KOR": "N Korea", "KWT": "Kuwait", "KGZ": "Kyrgyzstan", "LAO": "Lao", "LVA": "Latvia", "LBN": "Lebanon", "LSO": "Lesotho", "LBR": "Liberia", "LBY": "Libya", "LIE": "Liechten stein", "LTU": "Lithuania", "LUX": "Luxembourg", "MDG": "Madagascar", "MWI": "Malawi", "MYS": "Malaysia", "MDV": "Maldives", "MLI": "Mali", "MLT": "Malta", "MHL": "Marshall Islands", "MRT": "Mauritania", "MUS": "Mauritius", "MEX": "Mexico", "FSM": "Micronesia", "MDA": "Moldova", "MCO": "Monaco", "MNG": "Mongolia", "MNE": "Montenegro", "MAR": "Morocco", "MOZ": "Mozambique", "MMR": "Myanmar", "NAM": "Namibia", "NRU": "Nauru", "NPL": "Nepal", "NLD": "Nether lands", "NZL": "New Zealand", "NIC": "Nicaragua", "NER": "Niger", "NGA": "Nigeria", "MKD": "North Macedonia", "NOR": "Norway", "OMN": "Oman", "PAK": "Pakistan", "PLW": "Palau", "PAN": "Panama", "PNG": "Papua New Guinea", "PRY": "Paraguay", "PER": "Peru", "PHL": "Philippines", "POL": "Poland", "PRT": "Portugal", "QAT": "Qatar", "ROU": "Romania", "RUS": "Russian", "RWA": "Rwanda", "KNA": "Saint Kitts and Nevis", "LCA": "Saint Lucia", "VCT": "Saint Vincent and the Grenadines", "WSM": "Samoa", "SMR": "San Marino", "STP": "Sao Tome and Principe", "SAU": "Saudi Arabia", "SEN": "Senegal", "SRB": "Serbia", "SYC": "Seychelles", "SLE": "Sierra Leone", "SGP": "Singapore", "SVK": "Slovakia", "SVN": "Slovenia", "SLB": "Solomon", "SOM": "Somalia", "ZAF": "South Africa", "SSD": "South Sudan", "ESP": "Spain", "LKA": "Sri Lanka", "SDN": "Sudan", "SUR": "Suriname", "SWE": "Sweden", "CHE": "Switzer land", "SYR": "Syria", "TJK": "Tajikistan", "TZA": "Tanzania", "THA": "Thailand", "TLS": "Timor Leste", "TGO": "Togo", "TON": "Tonga", "TTO": "Trinidad and Tobago", "TUN": "Tunisia", "TUR": "Turkey", "TKM": "Turkmeni stan", "TUV": "Tuvalu", "UGA": "Uganda", "UKR": "Ukraine", "ARE": "UAE", "GBR": "United Kingdom", "USA": "USA", "URY": "Uruguay", "UZB": "Uzbekistan", "VUT": "Vanuatu", "VEN": "Venezuela", "VNM": "Viet Nam", "YEM": "Yemen", "ZMB": "Zambia", "ZWE": "Zimbabwe" }
    cntry = {}
    for temp in geo_json_data['features']:
        cntry[temp['id']] = temp['properties']['name']
    for country_alpha3 in cntry:
        if country_alpha3 not in summary_json['countriesSorted_Confirmed']:
            pass
        trend = {
            'confirmed': {},
            'recovered': {},
            'deaths': {}
        }
        print_info(f"Fetching records from DB for country[{country_alpha3}]..")
        records = Record.objects.filter(country_alpha3=country_alpha3).values_list('stats_type', 'stats_dates_csv', 'stats_value_csv')
        for record in records:
            stats_type  = record[0]
            dates_list  = record[1].split(",")
            values_list = record[2].split(",")
            for index, date_str in enumerate(dates_list):
                value = values_list[index]
                # date_obj = str(datetime.datetime.strptime(date_str, "%Y-%m-%d"))
                date_obj = str(datetime.strptime(date_str, "%Y-%m-%d"))
                if date_obj in trend[stats_type]:
                    trend[stats_type][date_obj] += int(value)
                else:
                    trend[stats_type][date_obj] = int(value)
        print_info(f"Fetching records from DB for country[{country_alpha3}]..Done")

        confirmed_dates_list  = list(trend['confirmed'].keys());confirmed_dates_list.sort()
        confirmed_values_list = list(trend['confirmed'].values());confirmed_values_list.sort()
        recovered_dates_list  = list(trend['recovered'].keys());recovered_dates_list.sort()
        recovered_values_list = list(trend['recovered'].values());recovered_values_list.sort()
        deaths_dates_list     = list(trend['deaths'].keys());deaths_dates_list.sort()
        deaths_values_list    = list(trend['deaths'].values());deaths_values_list.sort()

        print_info('Generating the plot..')

        data = {
            "data": [
                go.Scatter(
                    x=confirmed_dates_list,
                    y=confirmed_values_list,
                    mode='lines+markers',
                    name='Confirmed',
                    line=dict(color='#3366CC'),
                    marker=dict(
                        color='#003366',
                        size=4,
                        # line=dict(
                        #     color='MediumPurple',
                        #     width=2
                        # )
                    ),
                ),
                go.Scatter(
                    x=recovered_dates_list,
                    y=recovered_values_list,
                    mode='lines+markers',
                    name='Recovered',
                    line=dict(color='green'),
                    marker=dict(
                        color='darkgreen',
                        size=4
                    ),
                ),
                go.Scatter(
                    x=deaths_dates_list,
                    y=deaths_values_list,
                    mode='lines+markers',
                    name='Deaths',
                    line=dict(color='tomato'),
                    marker=dict(
                        color='red',
                        size=4,
                    ),
                )
            ],
            "layout": go.Layout(
                margin={'l': 0, 'r': 0, 't': 0, 'b': 0},
                paper_bgcolor='#ffffff',
                plot_bgcolor='rgba(0,0,0,0)',
                height=350,
                # title="Trend",
                autosize=True,
                # xaxis_title='Time',
                # yaxis_title='Count'
                legend_orientation="h",
                xaxis1={
                    "gridcolor": "rgba(209, 187, 149, .5)",
                    "zerolinecolor": "rgba(209, 187, 149, .8)"
                },
                yaxis1={
                    "gridcolor": "rgba(209, 187, 149, .5)",
                    "zerolinecolor": "rgba(209, 187, 149, .8)"
                },
            )
        }
        config = {'displayModeBar': False}

        plotly_div = plotly.offline.plot(data, include_plotlyjs=True, config=config, output_type='div')
        print_info('Generating the plot..Done')

        local_file_name = f"datasets/html/countries/{country_alpha3}/plotly.html"
        print_info(f"Writing generated plotly HTML in local file[{local_file_name}..")
        Path(f"datasets/html/countries/{country_alpha3}").mkdir(parents=True, exist_ok=True)
        try:
            with open(local_file_name, "w") as outfile:
                outfile.write(plotly_div)
        except Exception as e:
            print_error(f"Writing generated plotly HTML in local file[{local_file_name}]..Failed")


def store_country_stats_table_html():

    print_info("Generating stats table..")
    summary_json = json.loads(open('datasets/summary.json').read())
    geo_json_data = json.loads(open('datasets/GeoJsonWorldCountries.json').read())

    # Create a local simplified dict from topojson - like below
    # cntry = { "AFG": "Afghanistan", "ALB": "Albania", "DZA": "Algeria", ..}
    cntry = {}
    for temp in geo_json_data['features']:
        cntry[temp['id']] = temp['properties']['name']
    for country_alpha3 in cntry:
        if country_alpha3 not in summary_json['countriesSorted_Confirmed']:
            pass
        print_info(f"Generating stats HTML for [{country_alpha3}]..")
        records_list = []
        records = Record.objects.filter(country_alpha3=country_alpha3).values_list(
            'state_province',
            'country_region',
            'stats_type',
            'latest_stats_date',
            'latest_stats_value'
        )
        records_dict = {}
        for record in records:
            state_province     = record[0] or 'No data'
            country_region     = record[1]
            stats_type         = record[2]
            latest_stats_date  = record[3]
            latest_stats_value = record[4]
            if state_province not in records_dict:
                records_dict[state_province] = {}
            if stats_type not in records_dict[state_province]:
                records_dict[state_province][stats_type] = {}
            records_dict[state_province][stats_type]['latest_stats_date'] = latest_stats_date
            records_dict[state_province][stats_type]['latest_stats_value'] = latest_stats_value

        table_rows_html = ''
        for state in records_dict:
            try:
                confirmed = records_dict[state]['confirmed']['latest_stats_value']
            except KeyError:
                confirmed = '-'

            try:
                recovered = records_dict[state]['recovered']['latest_stats_value']
            except KeyError:
                recovered = '-'

            try:
                deaths = records_dict[state]['deaths']['latest_stats_value']
            except KeyError:
                deaths = '-'

            table_rows_html += f"<tr>"
            table_rows_html += f"<td>{state}</td>"
            table_rows_html += f"<td class='text-right text-warning'>{confirmed}</td>"
            table_rows_html += f"<td class='text-right text-success'>{recovered}</td>"
            table_rows_html += f"<td class='text-right text-danger'>{deaths}</td>"
            table_rows_html += f"</tr>"

        table_html = f'\
            <table id="table-country-records" class="table table-sm">\
                <thead>\
                <tr>\
                    <td>State</td>\
                    <td class="text-right text-warning">Confirmed</td>\
                    <td class="text-right text-success">Recovered</td>\
                    <td class="text-right text-danger">Deaths</td>\
                </tr>\
                </thead>\
                <tbody>{table_rows_html}</tbody>\
            </table>'
        print_info(f"Generating stats HTML for {country_alpha3}..Done")

        local_file_name = f"datasets/html/countries/{country_alpha3}/stats.html"
        print_info(f"Writing generated table HTML in local file[{local_file_name}..")
        Path(f"datasets/html/countries/{country_alpha3}").mkdir(parents=True, exist_ok=True)
        with open(local_file_name, "w") as outfile:
            outfile.write(table_html)
        print_info(f"Writing generated table HTML in local file[{local_file_name}]..Done")


def write_to_file_summary_json(summary):
    print_info("Writing summary to local file[summary.json]..")
    with open("datasets/summary.json", "w") as outfile:
        json.dump(summary, outfile)
    print_info("Writing summary to local file[summary.json]..Done")


def populate_planetary_file():
    # Store formatted data in a local file - to be consumed by planetaryjs
    # We are storing the data in file as reading data from DB is very slow
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


def truncate_records():

    print_info("Truncating [RECORD] table..")
    Record.objects.all().delete()
    print_info("Truncating [RECORD] table..Done")
