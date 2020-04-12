from django.shortcuts import render
from django.http import HttpResponse, JsonResponse, response
# Custom imports
from lib.common.console import print_info
from lib.common.utils import get_country_df_alpha3
from lib.country.utils import find_trend_country, fetch_country_records


def country_home(request, name='default'):
    print_info("Fetching country dataframes..")
    countries_df = get_country_df_alpha3()
    print_info("Fetching country dataframes..Done")

    print_info("Fetching graph html..")
    div_html = find_trend_country(country_alpha3=name)
    print_info("Fetching graph html..Done")

    print_info("Fetching states table html..")
    country_table_html = fetch_country_records(country_alpha3=name)
    print_info("Fetching states table html..Done")
    
    context = {
        'country': countries_df.loc[name]['Country'],
        'country_table_html': country_table_html,
        'div_html': div_html
    }
    return render(request, "country/country_home.html", context)
