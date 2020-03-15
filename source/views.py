from django.shortcuts import render
from django.http import HttpResponse, JsonResponse, response
from django.db.models import Sum
from django.core import serializers
import json
from datetime import datetime
# Custom imports
from core.models import Record


def coronafeed(request):
    details = {}
    details['totals']    = findSumAcrossAllCountries()['totals']
    details['countries'] = findSumAcrossEachCountry()['countries']
    details['countriesSorted_Deaths']    = findCountriesSorted(stats_type='deaths')
    details['countriesSorted_Recovered'] = findCountriesSorted(stats_type='recovered')
    details['countriesSorted_Confirmed'] = findCountriesSorted(stats_type='confirmed')
    return JsonResponse(details)


def findSumAcrossAllCountries():
    # Find totals of confirmed/deaths/recovered across ALL countires
    temp = {}
    temp['totals'] = {}
    deaths_total = Record.objects.filter(stats_type='deaths').aggregate(Sum('latest_stats_value'))
    confirmed_total = Record.objects.filter(stats_type='confirmed').aggregate(Sum('latest_stats_value'))
    recovered_total = Record.objects.filter(stats_type='recovered').aggregate(Sum('latest_stats_value'))
    temp['totals']['total_deaths']    = deaths_total['latest_stats_value__sum']
    temp['totals']['total_confirmed'] = confirmed_total['latest_stats_value__sum']
    temp['totals']['total_recovered'] = recovered_total['latest_stats_value__sum']
    return temp


def findSumAcrossEachCountry():
    temp = {}
    temp['countries'] = {}
    query = """
    SELECT
        1 AS ID,
        COUNTRY_REGION,
        STATS_TYPE,
        SUM(LATEST_STATS_VALUE) AS TOTAL
    FROM
        PUBLIC.CORE_RECORD
    GROUP BY
        STATS_TYPE,
        COUNTRY_REGION
    ORDER BY
        COUNTRY_REGION,
        STATS_TYPE"""
    querySet = Record.objects.raw(query)
    for rec in querySet:
        if (not(rec.country_region in temp['countries'])):
            temp['countries'][rec.country_region] = {}
        temp['countries'][rec.country_region][rec.stats_type] = rec.total
    return temp


def findCountriesSorted(stats_type):
    lst = []
    sql = "SELECT 1 as ID, COUNTRY_REGION, SUM(LATEST_STATS_VALUE) AS TOTAL FROM PUBLIC.CORE_RECORD WHERE STATS_TYPE='{}' GROUP BY COUNTRY_REGION ORDER BY TOTAL DESC".format(stats_type)
    qs = Record.objects.raw(sql)
    for p in qs[:5]:
        lst.append(p.country_region)
    return lst
