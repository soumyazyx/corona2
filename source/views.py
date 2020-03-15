import json
import datetime
from django.db.models import Sum
from django.shortcuts import render
from django.core import serializers
from django.http import HttpResponse, JsonResponse, response
# Custom imports
from core.models import Summary


def coronafeed(request):

    summary_qs = Summary.objects.all()
    for summary in summary_qs:
        json_string = summary.json_string
    obj = json.loads(json_string)
    return JsonResponse(obj)
