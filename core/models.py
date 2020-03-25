from django.db import models


class Record(models.Model):
    state_province     = models.CharField(max_length=255, default='')
    country_region     = models.CharField(max_length=255, default='')
    latitude           = models.CharField(max_length=255, default=0)
    longitude          = models.CharField(max_length=255, default=0)
    stats_type         = models.CharField(max_length=255, default='')
    stats_dates_csv    = models.TextField(default='')
    stats_value_csv    = models.TextField(default='')
    latest_stats_date  = models.CharField(max_length=255, default='')
    latest_stats_value = models.IntegerField(default=0)
    added_ts           = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return "{}|{}|{}".format(self.added_ts, self.stats_type, self.country_region)


class Summary(models.Model):
    
    added_ts = models.DateTimeField(auto_now_add=True)
    json_string = models.TextField(default='')

    def __str__(self):
        return "{}|{}".format(self.added_ts, self.json_string[:10])
