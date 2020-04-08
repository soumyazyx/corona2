from lib.common.console import print_info


def rectifyDateFormat(dates_csv):
    import datetime
    dates_list = []
    for date in dates_csv.split(","):

        month, day, year = date.split("/")
        year = '20' + year
        x = datetime.datetime(int(year), int(month), int(day))
        x = str(x.date())
        dates_list.append(x)
    dates_csv = ",".join(dates_list)
    return dates_csv
