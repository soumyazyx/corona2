from django.shortcuts import render
from django.http import HttpResponse
import folium


def show_map(request):
    # m = folium.Map([17.3850, 78.4867], zoom_start=10)
    m = folium.Map()
    map_html = m.get_root().render()
    context = {'map_html': map_html}
    return render(request, 'sample_map.html', context)
