import pandas as pd
import argparse
import xlsxwriter
import requests
import os
from datetime import datetime, date, time

import pdb


# служебные функции
def getIntervalTime(t1H, t1M, t2H, t2M):
    """выделение отрезка времени, используется для определения текущего отбора во вкладку bad МПП"""
    curdate = datetime.now()
    tekHour = curdate.hour
    tekMinute = curdate.minute
    if (tekHour == t1H) or (tekHour == t2H):
        if (t1M <= tekMinute) and (t2M >= tekMinute):
            return True
    return False


def getIntervalTime2(t1, t2, hour_zone):
    tmp = t1.split(":")
    t1H = int(tmp[0]) - hour_zone
    t1M = int(tmp[1])
    tmp = t2.split(":")
    t2H = int(tmp[0]) - hour_zone
    t2M = int(tmp[1])
    result = getIntervalTime(t1H, t1M, t2H, t2M)
    return result


def del_file(filename):
    # удаляет файл, если он существует
    if os.path.exists(filename):
        os.remove(filename)
    return True


# END служебные функции

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
    """
    suri = "http://voip.2gis.local/cisco-stat/cdr.php?s=1&t=&order=dateTimeOrigination&sens=DESC&current_page=0" \
           "&posted=1&current_page=0&fromstatsmonth={0}&tostatsmonth={1}&Period=Day&fromday=true" \
           "&fromstatsday_sday={2}&fromstatsmonth_sday={3}&today=true&tostatsday_sday={4}&tostatsmonth_sday={5}" \
           "&callingPartyNumber=&callingPartyNumbertype=1&originalCalledPartyNumber=%2B7" \
           "&originalCalledPartyNumbertype=2&origDeviceName=&origDeviceNametype=1&destDeviceName=" \
           "&destDeviceNametype=1&image16.x=27&image16.y=8&resulttype=min". \
        format(begyearmonth, endyearmonth, begday, begyearmonth, endday, endyearmonth)
    """
    suri = "http://10.54.16.20/cisco-stat/cdr.php?s=1&t=&order=dateTimeOrigination&sens=DESC&current_page=0" \
           "&posted=1&current_page=0&fromstatsmonth={0}&tostatsmonth={1}&Period=Day&fromday=true" \
           "&fromstatsday_sday={2}&fromstatsmonth_sday={3}&today=true&tostatsday_sday={4}&tostatsmonth_sday={5}" \
           "&callingPartyNumber=&callingPartyNumbertype=1&originalCalledPartyNumber=%2B7" \
           "&originalCalledPartyNumbertype=2&origDeviceName=&origDeviceNametype=1&destDeviceName=" \
           "&destDeviceNametype=1&image16.x=27&image16.y=8&resulttype=min". \
        format(begyearmonth, endyearmonth, begday, begyearmonth, endday, endyearmonth)
    #suri2 = "http://voip.2gis.local/cisco-stat/export_csv.php"
    suri2 = "http://10.54.16.20/cisco-stat/export_csv.php"
    print(suri)
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


def calc(begin_date, begin_time, end_date, end_time, filename, output_filename, name_file_cfg_tel):
    print("Start {} {} - {} {}".format(begin_date, begin_time, end_date, end_time))
    # загрузка информации лога звонков
    columns = ["Calldate", "Source", "Destination", "Disconnect Time", "origCause_value",
               "destCause_value", "origDeviceName", "destDeviceName", "outpulsedCallingPartyNumber",
               "outpulsedCalledPartyNumber", "Duration", "No"]
    dtypes = {"Calldate": "object", "Source": "object", "Destination": "object", "Disconnect Time": "object",
              "origCause_value": "object",
              "destCause_value": "object", "origDeviceName": "object", "destDeviceName": "object",
              "outpulsedCallingPartyNumber": "object",
              "outpulsedCalledPartyNumber": "object", "Duration": "int64", "No": "object"}
    log_zvonkov = pd.read_csv(filename, ';', header=None, names=columns, dtype=dtypes)
    new_log = log_zvonkov[["Calldate", "Source", "Destination", "Duration"]]  # выбираем только нужные нам поля таблицы
    # фильтрация по дате и времени
    begin_datetime = "{} {}".format(begin_date, begin_time)
    end_datetime = "{} {}".format(end_date, end_time)
    filter_date = (new_log["Calldate"] > begin_datetime) & (new_log["Calldate"] < end_datetime)
    new_log = new_log[filter_date]
    # END фильтрация по дате

    # загрузка информации о принадлежности номеров телефонов к конкретным менеджерам
    columns = ["Source", "FioMPP", "FioRg", "Plan result unik zvonok"]
    dtypes = {"Source": "object", "FioMPP": "object", "FioRg": "object", "Plan result unik zvonok": "int64"}
    list_cfg = pd.read_csv(name_file_cfg_tel, ';', header=None, names=columns, dtype=dtypes)
    # ---
    columns = ["Source", "FioMPP", "FioRg", "Plan result unik zvonok"]
    dtypes = {"Source": "object", "FioMPP": "object", "FioRg": "object", "Plan result unik zvonok": "int64"}
    list_cfg2 = pd.read_csv(name_file_cfg_tel, ';', header=None, names=columns, dtype=dtypes, index_col=0)
    # END загрузка информации о принадлежности номеров телефонов к конкретным менеджерам
    # print(list_cfg)
    data = new_log.merge(list_cfg, on="Source", how="left")
    # print(data)
    # data.to_csv("data.csv")
    data = data.dropna()  # удаление отсутствующих данных,таким образом отфильтровали номера которые нас не интересуют
    # data.to_csv("data-dropna.csv")

    # выборка общее кол-во набранных телефонов каждым из сотрудников
    group = data["Destination"].groupby([data["Source"]])
    # END выборка общее кол-во набранных телефонов каждым из сотрудников
    # подсчет уникальных звонков
    result_unuque = group.nunique()
    result_unuque.name = "Unique tel"
    # END подсчет уникальных звонков

    # выборка общее кол-во набранных телефонов каждым из сотрудников по результативным звонкам
    data_result_duration = data[data["Duration"] >= 20]
    group = data_result_duration["Destination"].groupby(data_result_duration["Source"])
    # END выборка общее кол-во набранных телефонов каждым из сотрудников по результативным звонкам

    # подсчет уникальных результативных звонков
    result_unuque_result = group.nunique()
    result_unuque_result.name = "Unique result tel"
    # END подсчет уникальных результативных звонков

    # объединение двух результов группировки
    result_frame = pd.concat([result_unuque, result_unuque_result], axis=1)
    result_frame.index = result_frame.index.astype("int64")
    # END объединение двух результов группировки

    # объединение результов группировки с итоговой таблицей
    result_frame2 = list_cfg2.join(result_frame)
    result_frame2 = result_frame2.fillna(0)
    # END объединение результов группировки с итоговой таблицей
    print("Done {} {} - {} {}".format(begin_date, begin_time, end_date, end_time))
    return result_frame2


def xlsx(workbook, td, name_sheet="лог звонков", plan_unik_result_tel="", flag_bad=False, add_name="", flag_polchasa=True, unique_result_tel_mean=0, unique_result_tel_mean_group=None):
    """выгрузка в файл эксель"""
    # flag_bad - флаг того выгружается ли в лист только плохие
    # flag_polchasa - флаг, того что выгружается результат в полчаса, то есть плановое кол-во результативных уникальных которое находится в 
    # файле конфигурации делится на 5 

    worksheet = workbook.get_worksheet_by_name(name_sheet)

    # формат для выделения внимания
    format_red = workbook.add_format()
    format_red.set_font_color('red')
    format_red.set_bg_color('white')
    format_red.set_border()
    format_red.set_text_wrap()
    format_red.set_align('vcenter')
    format_red.set_align('center')

    # формат для средних значений по звонкам
    format_mean = workbook.add_format()
    format_mean.set_font_color('black')
    format_mean.set_bg_color('white')
    format_mean.set_border()
    format_mean.set_text_wrap()
    format_mean.set_align('vcenter')
    format_mean.set_align('center')
    format_mean.set_text_wrap(True)
    format_mean.set_num_format("0.00")
    


    #  формат по умолчанию
    format_default = workbook.add_format()
    format_default.set_font_color('black')
    format_default.set_bg_color('white')
    format_default.set_border()
    format_default.set_text_wrap()
    format_default.set_align('vcenter')
    format_default.set_align('center')

    worksheet.set_column('A:A', 10)
    worksheet.set_column('B:B', 30)
    worksheet.set_column('C:C', 30)
    worksheet.set_column('D:D', 15)
    worksheet.set_column('E:E', 15)
    worksheet.set_column('F:F', 15)

    worksheet.set_row(1, 65, format_default)

    # заголовок таблицы
    worksheet.write(0, 0, "Плохие МПП за {}   - Выгружено: {}".format(add_name, datetime.now()))
    worksheet.write(1, 0, "номер телефона", format_default)
    worksheet.write(1, 1, "ФИО МПП", format_default)
    worksheet.write(1, 2, "ФИО РГ", format_default)
    worksheet.write(1, 3, "Кол-во\nуникальных\nзвонков", format_default)
    worksheet.write(1, 4, "Кол-во\nрезультативных\nуникальных\nзвонков", format_default)
    worksheet.write(1, 5, "Плановое\nкол-во\nрезультативных\nуникальных\nзвонков\nв получасе", format_default)

    # координаты откуда будет заполнять таблицу данными
    row = 2
    col = 0
    if flag_bad:
        for num_tel in td.index:
            fio_manager = (td["FioMPP"])[num_tel]
            fio_rg = (td["FioRg"])[num_tel]
            unik_tel = (td["Unique tel"])[num_tel]
            kol_uniq_result_tel = (td["Unique result tel"])[num_tel]
            if flag_polchasa:
                plan_unik_result_tel = (td["Plan result unik zvonok"])[num_tel] // 5
            else:
                plan_unik_result_tel = (td["Plan result unik zvonok"])[num_tel]
            if kol_uniq_result_tel >= plan_unik_result_tel:
                continue
            format = format_red
            worksheet.write(row, col, num_tel, format)
            worksheet.write(row, col + 1, fio_manager, format)
            worksheet.write(row, col + 2, fio_rg, format)
            worksheet.write(row, col + 3, unik_tel, format)
            worksheet.write(row, col + 4, kol_uniq_result_tel, format)
            worksheet.write(row, col + 5, plan_unik_result_tel, format)
            row += 1
    else:
        for num_tel in td.index:
            fio_manager = (td["FioMPP"])[num_tel]
            fio_rg = (td["FioRg"])[num_tel]
            unik_tel = (td["Unique tel"])[num_tel]
            kol_uniq_result_tel = (td["Unique result tel"])[num_tel]
            if flag_polchasa:
                plan_unik_result_tel = (td["Plan result unik zvonok"])[num_tel] // 5
            else:
                plan_unik_result_tel = (td["Plan result unik zvonok"])[num_tel]            
            if kol_uniq_result_tel >= plan_unik_result_tel:
                format = format_default
            else:
                format = format_red
            worksheet.write(row, col, num_tel, format)
            worksheet.write(row, col + 1, fio_manager, format)
            worksheet.write(row, col + 2, fio_rg, format)
            worksheet.write(row, col + 3, unik_tel, format)
            worksheet.write(row, col + 4, kol_uniq_result_tel, format)
            worksheet.write(row, col + 5, plan_unik_result_tel, format)
            row += 1

        # вывод средних значений по группам продаж и филиалу
        
        worksheet.set_column("I:I", 30)
        worksheet.set_column("J:J", 30)
        
        worksheet.set_row(1,100)
        worksheet.set_row(5,40)
        

        
        worksheet.write(1, 8, "Среднее кол-во результир. уникальных звонков по филиалу", format_mean)
        #pdb.set_trace()

        worksheet.write(2, 8, unique_result_tel_mean, format_mean)

        worksheet.write(5, 8, "Среднее зн-ние результир. уникальных звонков по группам", format_mean)
        worksheet.write(6, 8, "ФИО РГ", format_mean)
        worksheet.write(6, 9, "Сред. зн-ние", format_mean)
        row = 1

        if (unique_result_tel_mean_group is not None):
            for k, item in unique_result_tel_mean_group.items():
                worksheet.write(6+row, 8, k, format_mean)
                worksheet.write(6+row, 9, item, format_mean)
                row = row + 1
            #pdb.set_trace()




    return True

def run_log_zvonkov_new(begin_date, end_date, namefile_xlsx, name_file_cfg):
    # параметры программы
    print(name_file_cfg)
    # загрузка информации о принадлежности номеров телефонов к конкретным менеджерам
    columns = ["Source", "FioMPP", "FioRg", "Plan result unik zvonok"]
    dtypes = {"Source": "object", "FioMPP": "object", "FioRg": "object", "Plan result unik zvonok": "int64"}
    list_cfg = pd.read_csv(name_file_cfg, ';', header=None, names=columns, dtype=dtypes)
    
    #print(list_cfg["Plan result unik zvonok"][15])

    
    plan_count_result_zvonok = 25
    #plan_result_zvonok = 20  # продолжительность результативного звонка
    # report_filename = "Reports.csv"  # файл куда сохраняются сырые данные лога звонков для последующей обработки
    hour_zone = 4  # часовая разница с Новосибирском по сравнению с локальным временем
    # END параметры программы
    namefile_xlsx = namefile_xlsx

    try:
        report_filename = get_data_from_server(begin_date, end_date)
    except Exception:
        # для теста
        report_filename = "report-2017-11-01-2017-11-16.csv"
        # END для теста

    interval_time = (
        ("13:00:00", "23:59:59"), ("13:00:00", "13:29:59"), ("13:30:00", "13:59:59"), ("14:00:00", "14:29:59"),
        ("14:30:00", "14:59:59"),
        ("15:00:00", "15:29:59"), ("15:30:00", "15:59:59"), ("16:00:00", "23:59:59"))
    name_sheets = ("лог звонков(итоговый)", "время 9-00 до 9-30", "время 9-30 до 10-00", "время 10-00 до 10-30",
                   "время 10-30 до 11-00",
                   "время 11-00 до 11-30", "время 11-30 до 12-00", "время 12-00 до 23-59")

    workbook = xlsxwriter.Workbook(namefile_xlsx)
    # создаем листы в книге экселя
    workbook.add_worksheet("BAD МПП")
    for i in range(len(name_sheets)):
        workbook.add_worksheet(name_sheets[i])

    #print(result_log)

    kol_pol4asa = 0 # кол-во пройденных получасов которое используется для средних значений в итоговом листе

    for i in range(1, len(interval_time)):
        result_log = calc(begin_date, interval_time[i][0], end_date, interval_time[i][1], filename=report_filename,
                          output_filename=namefile_xlsx, name_file_cfg_tel=name_file_cfg)
        #print(result_log)
        # удаление строк где не было звонков МПП 
        result_log_non_null = result_log[result_log["Unique tel"]!=0]
        print(result_log_non_null)
        if  result_log_non_null.empty:
            continue

        unique_result_tel_mean = result_log_non_null["Unique result tel"].mean()
        unique_result_tel_mean_group = result_log_non_null.groupby("FioRg").mean()["Unique result tel"]
        """
        else:
            unique_result_tel_mean = 0
            unique_result_tel_mean_group = None
        """

        #pdb.set_trace()
        xlsx(workbook, result_log, name_sheets[i], plan_count_result_zvonok // 5, flag_polchasa=True, unique_result_tel_mean = unique_result_tel_mean, unique_result_tel_mean_group=unique_result_tel_mean_group)
        if getIntervalTime2(interval_time[i][0], interval_time[i][1], hour_zone):
            if not ((i == 1)):
                result_log = calc(begin_date, interval_time[i - 1][0], end_date, interval_time[i - 1][1],
                                  filename=report_filename, output_filename=namefile_xlsx,
                                  name_file_cfg_tel=name_file_cfg)
                xlsx(workbook, result_log, "BAD МПП", plan_count_result_zvonok // 5,
                     True, "Плохие МПП за " + name_sheets[i - 1], flag_polchasa=True)

                kol_pol4asa = i

    #pdb.set_trace()   

    print("Кол-во получасов: ", kol_pol4asa)

    result_log = calc(begin_date, interval_time[0][0], end_date, interval_time[0][1], filename=report_filename,
                    output_filename=namefile_xlsx, name_file_cfg_tel=name_file_cfg)
    result_log_non_null = result_log[result_log["Unique tel"]!=0]
    #print(result_log_non_null)
    if  not result_log_non_null.empty  and kol_pol4asa !=0:
        kol_pol4asa = kol_pol4asa - 1
        unique_result_tel_mean = result_log_non_null["Unique result tel"].mean()/kol_pol4asa
        unique_result_tel_mean_group = result_log_non_null.groupby("FioRg").mean()["Unique result tel"]/kol_pol4asa
    else:
        unique_result_tel_mean = result_log_non_null["Unique result tel"].mean()
        unique_result_tel_mean_group = result_log_non_null.groupby("FioRg").mean()["Unique result tel"]
    xlsx(workbook, result_log, name_sheets[0], list_cfg, flag_polchasa=False, unique_result_tel_mean = unique_result_tel_mean, unique_result_tel_mean_group=unique_result_tel_mean_group)

    workbook.close()
    del_file(report_filename)
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-begindate", help="начальная дата отбора")
    parser.add_argument("-enddate", help="конечная дата отбора")
    parser.add_argument("-cfgfile", help="путь до списка телефонов которые нужно выгрузить в лог звонков")
    args = parser.parse_args()

    begin_date = args.begindate
    end_date = args.enddate
    name_file_cfg = args.cfgfile
    if name_file_cfg is None:
        name_file_cfg = 'list-num-tel.cfg'
    if (begin_date == None) or (end_date == None):
        begin_date = str(datetime.now().date())
        end_date = str(datetime.now().date())
    print("begin_date = ", begin_date)
    print("end_date = ", end_date)
    # для теста
    #begin_date = str(datetime.now().date().year)+"-"+str(datetime.now().date().month)+"-01"
    #end_date = str(datetime.now().date().year)+"-"+str(datetime.now().date().month)+"-01"
    print("begin_date = ", begin_date)
    print("end_date = ", end_date)     
    # END для теста
    namefile = "logs-{} - {}.xlsx".format(begin_date, end_date)
    run_log_zvonkov_new(begin_date, end_date, namefile, name_file_cfg)
