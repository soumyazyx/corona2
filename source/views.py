import json
import datetime
from django.http import JsonResponse
# Custom imports
from core.models import Summary
from lib.common.console import print_info


def coronafeed(request):
    print_info("Reading summary json from [datasets/summary.json]..")
    summary_json = json.loads(open('datasets/summary.json').read())
    print_info("Reading summary json from [datasets/summary.json]..Done")
    return JsonResponse(summary_json)
