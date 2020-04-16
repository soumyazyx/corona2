import pandas as pd
import datetime
import plotly
import plotly.graph_objs as go
# Custom imports
from core.models import Record, Summary
from lib.common.console import print_info


def find_trend_country(country_alpha3):

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
            date_obj = str(datetime.datetime.strptime(date_str, "%Y-%m-%d"))
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

    div = plotly.offline.plot(data, include_plotlyjs=True, config=config, output_type='div')
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
        confirmed = records_dict[state]['confirmed']['latest_stats_value']
        recovered = records_dict[state]['recovered']['latest_stats_value']
        deaths    = records_dict[state]['deaths']['latest_stats_value']

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

    return table_html
