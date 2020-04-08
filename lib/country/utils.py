import csv
import pandas as pd
import sqlalchemy
import psycopg2
import datetime
import plotly
import plotly.graph_objs as go
# Custom imports
from core.models import Record, Summary
from lib.common.console import print_info


def readTableIntoDFs(alpha3):

    print(datetime.datetime.now())
    engine = sqlalchemy.create_engine("postgresql+psycopg2://lfhryaadvfcvco:5b11e2552491ee96af5fafed86989774b5d9f954266786db2000e152a33cb68f@ec2-34-195-169-25.compute-1.amazonaws.com:5432/d5bbbijocpu6hk")
    query = """
    select
    *
    from
    public.core_record
    where
    country_alpha3 = '{}'
    """.format(alpha3)
    df = pd.read_sql_query(query, engine)
    print(datetime.datetime.now())


    # df = pd.read_sql_table('core_record', engine)
    # print(datetime.datetime.now())
    # print(df.shape)

    # df = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
    # print(df.shape)
    # print(datetime.datetime.now())

    # temp = {}
    #     temp['countries'] = {}
    #     query = """
    #     SELECT *
    #         1 AS ID,
    #         STATE_PROVINCE
    #         COUNTRY_REGION
    #         LATITUDE
    #         LONGITUDE
    #         STATS_TYPE
    #         STATS_DATES_CSV
    #         STATS_VALUE_CSV
    #         LATEST_STATS_DATE
    #         LATEST_STATS_VALUE
    #         ADDED_TS
    #     FROM
    #         PUBLIC.CORE_RECORD
    #     """
    #     querySet = Record.objects.raw(query)
    #     for rec in querySet:
    #         if (not(rec.country_region in temp['countries'])):
    #             temp['countries'][rec.country_region] = {}
    #         temp['countries'][rec.country_region][rec.stats_type] = rec.total
    #     return temp


def find_trend_country(country_alpha3):

    print_info(f"Fetching records from DB for country[{country_alpha3}]..")
    records = Record.objects.filter(country_alpha3=country_alpha3).values_list('stats_type','stats_dates_csv','stats_value_csv')
    print_info(f"Fetching records from DB for country[{country_alpha3}]..Done")
    trend = {
        'confirmed': {},
        'recovered': {},
        'deaths': {}
    }

    for record in records:
        stats_type  = record[0]
        dates_list  = record[1].split(",")
        values_list = record[2].split(",")
        for index, date_str in enumerate(dates_list):
            value = values_list[index]
            date_obj = str(datetime.datetime.strptime(date_str, "%Y-%m-%d"))
            if date_obj in trend[stats_type]:
                trend[stats_type][date_obj] += int(value)
            else:
                trend[stats_type][date_obj] = int(value)

    confirmed_dates_list = list(trend['confirmed'].keys());confirmed_dates_list.sort()
    confirmed_values_list = list(trend['confirmed'].values());confirmed_values_list.sort()

    recovered_dates_list = list(trend['recovered'].keys());recovered_dates_list.sort()
    recovered_values_list = list(trend['recovered'].values());recovered_values_list.sort()

    deaths_dates_list = list(trend['deaths'].keys());deaths_dates_list.sort()
    deaths_values_list = list(trend['deaths'].values());deaths_values_list.sort()

    print(f'{datetime.datetime.now()}: Generating the plot..')

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

            # title="Trend",
            # autosize=True,
            # xaxis_title='Time',
            # yaxis_title='Count'
            legend_orientation="h",
            # displayModeBar=False
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

    div = plotly.offline.plot(data, include_plotlyjs=True, output_type='div')
    print(f'{datetime.datetime.now()}: Generating the plot..Done')
    return div


def fetch_country_records(country_alpha3):
    
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
        state_province     = record[0]
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

    return records_dict