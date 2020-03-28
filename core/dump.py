
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
# ,Unemployment
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


def home_test_geojsontooltip(request):

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

    folium.GeoJson(
        geo_json_data,
        style_function=lambda feature: {
            'fillColor': color_dict[feature['id']],
            'color': 'black',
            'weight': 1,
            'dashArray': '5, 5',
            'fillOpacity': 0.9,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=['name'],
            aliases=['name'], 
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


