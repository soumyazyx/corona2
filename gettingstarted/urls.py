import hello.views
from django.contrib import admin
from django.urls import path, include
# Custom imports
from core.views   import sync, home, home_test
from source.views import coronafeed
from maps.views   import show_map


admin.autodiscover()
urlpatterns = [
    path("admin/", admin.site.urls),

    path("",               home,       name="core_home"),
    path("test/",          home_test,  name="core_home_test"),
    path("sync/",          sync,       name="core_sync"),
    path("api/coronafeed", coronafeed, name="corona-feed"),

    path("maps/world",     show_map,   name="show_map")

]