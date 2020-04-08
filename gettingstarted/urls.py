from django.contrib import admin
from django.urls import path, include
# Custom imports
from core.views import sync, home, home_test
from country.views import country_home
from source.views import coronafeed

admin.autodiscover()
urlpatterns = [
    path("admin/",         admin.site.urls),
    path("",               home,           name="core_home"),
    path("test/",          home_test,      name="core_home_test"),
    path("sync/",          sync,           name="core_sync"),
    path("api/coronafeed", coronafeed,     name="corona-feed"),
    path("country/",       country_home,   name="country-home"),
    path("country/<name>", country_home,   name="country-home")
]
