
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


def home_20200403(request):

    model_values = Record.objects.all().filter(stats_type='deaths').values(
        'latitude',
        'longitude',
        'country_region',
        'latest_stats_value'
    )

    summary_feed = requests.get('https://coronazyx.herokuapp.com/api/coronafeed')
    summary_json = summary_feed.json()

    print("{}: Fetching geojson data".format(datetime.now()))
    # geo_json_data = json.loads(requests.get('https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/world-countries.json').text)
    geo_json_data = json.loads(requests.get('https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/world-countries.json').text)
    print("{}: Fetching geojson data..Done".format(datetime.now()))
    return HttpResponse("wow")
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
            print(country)

    TESTDATA = StringIO(beep)
    unemployment_df = pd.read_csv(
        TESTDATA,
        sep="|",
        names=["State","Unemployment"]
    )

    linearrrr = cm.LinearColormap(
        ['#fac4c4','#f8302e'],
        vmin=unemployment_df.Unemployment.min(),
        vmax=unemployment_df.Unemployment.max()
    )

    unemployment_dict = unemployment_df.set_index('State')['Unemployment']
    color_dict = {key: linearrrr(unemployment_dict[key]) for key in unemployment_dict.keys()}

    color_dict['USA'] = color_dict['US']
    m = folium.Map([20.5937, 78.9629], zoom_start=1)

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
    map_html = m.get_root().render()
    context = {
        'map_html': map_html,
        "data": list(model_values),
        "summary": summary_feed.json()
    }
    return render(request, "index.html", context)

