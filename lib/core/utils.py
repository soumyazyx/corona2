import pandas as pd
import folium
from folium import plugins
from folium.features import GeoJson, GeoJsonTooltip
import branca.colormap as cm
from io import StringIO
# Custom imports
from lib.common.console import print_info
from lib.common.utils import get_country_dataframes


def handleChoroplethMap(summary_json, geo_json_data):

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
    return choropleth_map_html


def get_counts_table_html(summary_json, geo_json_data):

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
    return table_html


def populateWorldRecords(url, stats_type, countries_df):

    print("{}:Fetching content for stats_type [{}]..".format(datetime.now(), stats_type))
    print("{}:URL in use [{}]".format(datetime.now(), url))
    with requests.Session() as s:
        download = s.get(url)
    decoded_content = download.content.decode('utf-8')

    rows_fetched = list(csv.reader(decoded_content.splitlines(), delimiter=','))
    print("{}:Fetching content for stats_type [{}]..Done".format(datetime.now(), stats_type))

    # Handle header row
    header_row = rows_fetched.pop(0)  # Header is the first row.
    header_row.pop(0)  # Remove the value 'Province/State'
    header_row.pop(0)  # Remove the value 'Country/Region'
    header_row.pop(0)  # Remove the value 'Lat'
    header_row.pop(0)  # Remove the value 'Long'
    latest_stats_date = rectifyDateFormat(header_row[-1])
    # stats_dates_csv   = 
    stats_dates_csv   = rectifyDateFormat(dates_csv=(",".join(header_row)))

    print("{}:Creating objects..".format(datetime.now()))
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

    print("{}:Creating objects..Done".format(datetime.now()))
    print("{}:Total objects created = {}".format(datetime.now(), len(objects_list)))

    print("{}:Inserting records for stats_type[{}]..".format(datetime.now(), stats_type))
    Record.objects.bulk_create(objects_list)
    print("{}:Inserting records for stats_type[{}]..Done".format(datetime.now(), stats_type))
    return decoded_content
