import pandas as pd
import numpy as np


def get_data_from_server(begin_date, end_date):
    # скачивание данных в определенные даты из сайта данных, возвращает имя файла с полным путем
    print("Start get_data_from_server: {} - {}".format(begin_date, end_date))
    b_d = begin_date.split("-")
    e_d = end_date.split("-")
    begyearmonth = "{}-{}".format(b_d[0], b_d[1])
    endyearmonth = "{}-{}".format(e_d[0], e_d[1])
    begday = b_d[2]
    endday = e_d[2]
    if begin_date == end_date:
        # path_dirs = "{}/{}/{}".format(b_d[0], b_d[1], b_d[2])
        # os.makedirs(path_dirs, exist_ok=True)
        report_filename = "{}-log-zvonkov.csv".format(begin_date)
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
    return report_filename


interval_time = (("13:00", "23:59"), ("13:00", "13:29"), ("13:30", "13:59"), ("14:00", "14:29"), ("14:30", "14:59"),
                 ("15:00", "15:29"), ("15:30", "15:59"), ("16:00", "23:59"))
name_sheets = ("лог звонков(итоговый)", "время 9-00 до 9-30", "время 9-30 до 10-00", "время 10-00 до 10-30",
               "время 10-30 до 11-00",
               "время 11-00 до 11-30", "время 11-30 до 12-00", "время 12-00 до 23-59")


def calc(begin_date, begin_time, end_date, end_time):
    print("Start")
    # загрузка информации лога звонков
    # columns = {0: "Calldate",1: "Source",2: "Destination",3:"Disconnect Time",4:"origCause_value",5:"destCause_value",6:"origDeviceName",7:"destDeviceName",8:"outpulsedCallingPartyNumber",9:"outpulsedCalledPartyNumber",10:"Duration"}
    columns = ["Calldate", "Source", "Destination", "Disconnect Time", "origCause_value",
               "destCause_value", "origDeviceName", "destDeviceName", "outpulsedCallingPartyNumber",
               "outpulsedCalledPartyNumber", "Duration", "No"]
    dtypes = {"Calldate": "object", "Source": "str", "Destination": "object", "Disconnect Time": "object",
              "origCause_value": "object",
              "destCause_value": "object", "origDeviceName": "object", "destDeviceName": "object",
              "outpulsedCallingPartyNumber": "object",
              "outpulsedCalledPartyNumber": "object", "Duration": "int64", "No": "object"}
    log_zvonkov = pd.read_csv('report-2017-11-01-2017-11-16.csv', ';', header=None, names=columns, dtype=dtypes)
    new_log = log_zvonkov[["Calldate", "Source", "Destination", "Duration"]]  # выбираем только нужные нам поля таблицы
    # new_log.head()
    # new_log.dtypes
    # new_log.to_csv("logs.csv")

    # фильтрация по дате и времени
    begin_date = "2017-11-02"
    end_date = "2017-11-03"

    begin_datetime = "{} 00:00:01".format(begin_date)
    end_datetime = "{} 23:59:59".format(end_date)

    filter_date = (new_log["Calldate"] > begin_datetime) & (new_log["Calldate"] < end_datetime)
    new_log = new_log[filter_date]

    # END фильтрация по дате

    # загрузка информации о принадлежности номеров телефонов к конкретным менеджерам
    columns = ["Source", "FioMPP", "FioRg", "Plan_result_unik_zvonok", ""]
    dtypes = {"Source": "str", "FioMPP": "object", "FioRg": "object", "Plan_result_unik_zvonok": "int64"}
    list_cfg = pd.read_csv('list-num-tel.cfg', ';', header=None, names=columns, dtype=dtypes)
    list_cfg = list_cfg[columns[:-1]]  # отсекаем последний столбец
    list_cfg.head()

    data = new_log.merge(list_cfg, on="Source", how="left")
    data.head()
    data = data.dropna()  # удаление отсутствующих данных,таким образом отфильтровали номера которые нас не интересуют
    data.to_csv("logs-fil.csv")
    data.head()

    # выборка общее кол-во набранных телефонов каждым из сотрудников
    group = data["Destination"].groupby(data["Source"])
    group.count()
    # END выборка общее кол-во набранных телефонов каждым из сотрудников

    # подсчет уникальных звонков
    group.nunique()
    # END подсчет уникальных звонков
    print("Done")
    return


def run_log_zvonkov(begin_date, end_date, namefile_xlsx):
    # параметры программы
    plan_count_result_zvonok = 5
    plan_result_zvonok = 20  # продолжительность результативного звонка
    report_filename = "Reports.csv"  # файл куда сохраняются сырые данные лога звонков для последующей обработки
    hour_zone = 4  # часовая разница с Новосибирском по сравнению с локальным временем
    # END параметры программы

    report_filename = get_data_from_server(begin_date, end_date)
    print(report_filename)

    # workbook = xlsxwriter.Workbook(namefile_xlsx)
    interval_time = (("13:00", "23:59"), ("13:00", "13:29"), ("13:30", "13:59"), ("14:00", "14:29"), ("14:30", "14:59"),
                     ("15:00", "15:29"), ("15:30", "15:59"), ("16:00", "23:59"))
    name_sheets = ("лог звонков(итоговый)", "время 9-00 до 9-30", "время 9-30 до 10-00", "время 10-00 до 10-30",
                   "время 10-30 до 11-00",
                   "время 11-00 до 11-30", "время 11-30 до 12-00", "время 12-00 до 23-59")

    calc(begin_date, interval_time[0][0], end_date, interval_time[0][1])

    # workbook.add_worksheet("BAD МПП")

    # # создаем листы в книге экселя
    # for i in range(len(name_sheets)):
    #     workbook.add_worksheet(name_sheets[i])

    # # блок расчета показателей в указанный промежуток времени
    # calc(table_data, input_data, plan_result_zvonok, begin_date, interval_time[0][0], end_date, interval_time[0][1])
    # xlsx(workbook, table_data, name_sheets[0], plan_count_result_zvonok * 5)
    # for k in table_data:
    #     table_data[k].clear_calc()
    # # END - блок расчета показателей в указанный промежуток времени
    #
    # for i in range(1, len(interval_time)):
    #     # блок расчета показателей в указанный промежуток времени
    #     calc(table_data, input_data, plan_result_zvonok, begin_date, interval_time[i][0], end_date, interval_time[i][1])
    #     xlsx(workbook, table_data, name_sheets[i], plan_count_result_zvonok)
    #     for k in table_data:
    #         table_data[k].clear_calc()
    #     # END - блок расчета показателей в указанный промежуток времени
    #     if getIntervalTime2(interval_time[i][0], interval_time[i][1], hour_zone):
    #         if not ((i == 1)):
    #             calc(table_data, input_data, plan_result_zvonok, begin_date, interval_time[i - 1][0], end_date,
    #                  interval_time[i - 1][1])
    #             xlsx(workbook, table_data, "BAD МПП", plan_count_result_zvonok, True,
    #                  "Плохие МПП за " + name_sheets[i - 1])
    #             for k in table_data:
    #                 table_data[k].clear_calc()
    # # workbook.close()
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-begindate", help="начальная дата отбора")
    parser.add_argument("-enddate", help="конечная дата отбора")
    args = parser.parse_args()

    begin_date = args.begindate
    end_date = args.enddate
    if (begin_date == None) or (end_date == None):
        begin_date = str(datetime.now().date())
        end_date = str(datetime.now().date())
    print("begin_date = ", begin_date)
    print("end_date = ", end_date)
    # для теста
    # begin_date = "2017-10-30"
    # end_date = "2017-11-01"
    # END для теста
    namefile = "logs-{} - {}.xlsx".format(begin_date, end_date)
    run_log_zvonkov(begin_date, end_date, namefile)
