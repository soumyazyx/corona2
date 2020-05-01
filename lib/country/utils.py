import pandas as pd
import datetime
import plotly
import plotly.graph_objs as go
# Custom imports
from core.models import Record, Summary
from lib.common.console import print_info


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
