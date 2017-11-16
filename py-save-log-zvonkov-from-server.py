""" скачивание данных с сервера за указанный год, ежемесячно и ежедневно для создания архива"""

import requests
import os
import time


def get_data_from_server(begin_date, end_date):
    # скачивание данных в определенные даты из сайта данных
    print("Start get_data_from_server: {} - {}".format(begin_date, end_date))
    b_d = begin_date.split("-")
    e_d = end_date.split("-")
    begyearmonth = "{}-{}".format(b_d[0], b_d[1])
    endyearmonth = "{}-{}".format(e_d[0], e_d[1])
    begday = b_d[2]
    endday = e_d[2]
    if begin_date == end_date:
        path_dirs = "{}/{}/{}".format(b_d[0], b_d[1], b_d[2])
        os.makedirs(path_dirs, exist_ok=True)
        report_filename = "{}/{}-log-zvonkov.csv".format(path_dirs, begin_date)
    else:
        report_filename = "report-{}-{}.csv".format(begin_date, end_date)
    suri = "http://voip.2gis.local/cisco-stat/cdr.php?s=1&t=&order=dateTimeOrigination&sens=DESC&current_page=0" \
           "&posted=1&current_page=0&fromstatsmonth={0}&tostatsmonth={1}&Period=Day&fromday=true" \
           "&fromstatsday_sday={2}&fromstatsmonth_sday={3}&today=true&tostatsday_sday={4}&tostatsmonth_sday={5}" \
           "&callingPartyNumber=&callingPartyNumbertype=1&originalCalledPartyNumber=%2B7" \
           "&originalCalledPartyNumbertype=2&origDeviceName=&origDeviceNametype=1&destDeviceName=" \
           "&destDeviceNametype=1&image16.x=27&image16.y=8&resulttype=min". \
        format(begyearmonth, endyearmonth, begday, begyearmonth, endday, endyearmonth)
    suri2 = "http://voip.2gis.local/cisco-stat/export_csv.php"
    try:
        r = requests.get(suri)
        if r.status_code == 200:
            session_cook = r.headers['Set-Cookie']
            id_cookie = (session_cook.split(";"))[0]
            header_session = {'user-agent': 'py-log-zvonkov/0.0.1', 'Cookie': id_cookie}
            r = requests.get(suri2, headers=header_session)
            with open(report_filename, "w", encoding="utf8") as f:
                f.write(r.text)
    except requests.exceptions.ConnectionError:
        print("Сервер недоступен")
    print("Done get_data_from_server: {} - {}".format(begin_date, end_date))
    # END - скачивание данных в определенные даты из сайта данных


for i in range(1, 12):
    for j in range(1, 32):
        tekdata = "2017-{}-{}".format(i, j)
        get_data_from_server(begin_date=tekdata, end_date=tekdata)
        time.sleep(2)
