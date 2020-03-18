import hello.views
from django.contrib import admin
from django.urls import path, include
# Custom imports
from core.views   import sync, home
from source.views import coronafeed

admin.autodiscover()
urlpatterns = [
    path("admin/", admin.site.urls),

    path("",               home,       name="core_home"),
    path("sync/",          sync,       name="core_sync"),
    path("api/coronafeed", coronafeed, name="corona-feed")
]