from django.shortcuts import render
from django.http import HttpResponse, JsonResponse, response
# Custom imports
from lib.country.utils import find_trend_country, fetch_country_records


def country_home(request, name='default'):
    div_html = find_trend_country(country_alpha3=name)
    records_dict = fetch_country_records(country_alpha3=name)
    context = {
        'country': name,
        'records': records_dict,
        'div_html': div_html
    }
    return render(request, "country/country_home.html", context)
