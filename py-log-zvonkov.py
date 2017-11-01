import csv
import xlsxwriter
from datetime import datetime, date, time
import requests
import argparse


def getIntervalTime(t1H, t1M, t2H, t2M):
    """выделение отрезка времени, используется для определения текущего отбора во вкладку bad МПП"""
    curdate = datetime.now()
    tekHour = curdate.hour
    tekMinute = curdate.minute
    if (tekHour == t1H) or (tekHour == t2H):
        if (t1M <= tekMinute) and (t2M >= tekMinute):
            return True
    return False


class BaseDataTable:
    """ данные для хранения данных"""

    def __init__(self):
        self.data = {}

    def __getitem__(self, key):
        result = None
        if key in self.data:
            result = self.data[key]
        return result

    def __setitem__(self, key, value):
        if key not in self.data:
            self.data[key] = []
        self.data[key].append(value)

    def len(self):
        return len(self.data)


class InputData:
    """ структура входящих данных лога звонков"""
    # индексы полей, которые соответствуют колонкам в исходном csv-файле
    ind_datetime = 0  # дата и время начала звонка
    ind_source_tel = 1  # источник звонка (внутренний номер)
    ind_dest_tel = 2  # номер телефона куда звонили
    ind_secs = 10  # продолжительность звонка в секундах

    def __init__(self, datatimes, tel_dest, secs):
        self.datatimes = datatimes
        # self.tel_source = tel_source
        self.tel_dest = tel_dest
        self.secs = secs

    def __str__(self):
        result = "Дата: {}\nЦель: {}\nПродолжительность: {}\n".format(self.datatimes, self.tel_dest, self.secs)
        return result

    @classmethod
    def from_tuple(cls, row):
        """ Метод для создания экземпляра InputData
            из строки csv-файла"""
        return cls(
            datetime.strptime(row[cls.ind_datetime], "%Y-%m-%d %H:%M:%S"),
            # row[cls.ind_source_tel],
            row[cls.ind_dest_tel],
            row[cls.ind_secs],
        )


class TableData:
    """ итоговые данные"""
    # индексы полей, которые соответствуют колонкам в исходном csv-файле
    ind_num_tel = 0  # номер телефона МПП
    ind_fio_manager = 1  # ФИО МПП
    ind_fio_rg = 2  # ФИО РГ
    ind_plan_count_result_unik_tel = 3  # плановое кол-во уникальных результативных звоноков

    def __init__(self, fio_manager, fio_rg, plan_count_result_unik_tel=0):
        # self.num_tel = num_tel
        self.fio_manager = fio_manager
        self.fio_rg = fio_rg
        self.total_sec = 0  # общая продолжительность звонков (в сек)
        self.total_call = 0  # общее кол-во звоноков
        self.count_result_call = 0  # кол-во результативных звоноков
        self.count_unik_tel = 0  # кол-во уникальных телефонных номеров
        self.count_result_unik_tel = 0  # кол-во уникальных результативных звоноков
        self.result_sec = 0  # продолжительность результативных звонков (в сек)
        self._unik_tel = list()  # список уникальных номеров телефонов
        self._result_unik_tel = list()  # список уникальных номеров телефонов по которым совершен результативный звонок
        # плановые показатели по МПП
        self.plan_count_result_unik_tel = plan_count_result_unik_tel  # плановое кол-во уникальных результативных звоноков

    def clear_calc(self):
        """ очистка данных которые вычисляются"""
        self.total_sec = 0  # общая продолжительность звонков (в сек)
        self.total_call = 0  # общее кол-во звоноков
        self.count_result_call = 0  # кол-во результативных звоноков
        self.count_unik_tel = 0  # кол-во уникальных телефонных номеров
        self.count_result_unik_tel = 0  # кол-во уникальных результативных звоноков
        self.result_sec = 0  # продолжительность результативных звонков (в сек)
        self._unik_tel = list()  # список уникальных номеров телефонов
        self._result_unik_tel = list()  # список уникальных номеров телефонов по которым совершен результативный звонок

    @classmethod
    def from_tuple(cls, row):
        """ Метод для создания экземпляра TableData
            из строки csv-файла"""
        return cls(
            # row[cls.ind_num_tel],
            row[cls.ind_fio_manager],
            row[cls.ind_fio_rg],
            row[cls.ind_plan_count_result_unik_tel]
        )

    @staticmethod
    def sec_to_hour(ss):
        return int(ss / 3600)

    @staticmethod
    def sec_to_min(ss):
        return int(ss / 60)

    @staticmethod
    def sec_to_s(s):
        hh = TableData.sec_to_hour(s)
        mm = TableData.sec_to_min(s - hh * 3600)
        ss = s - mm * 60 - hh * 3600
        return "{}:{}:{}".format(hh, mm, ss)

    def __str__(self):
        result = "ФИО МПП: {}\nФИО РГ: {}\nTotal_sec: {}\nUnik_tel: {}\nResult_Unik_tel: {}".format(self.fio_manager,
                                                                                                    self.fio_rg,
                                                                                                    TableData.sec_to_s(
                                                                                                        self.total_sec),
                                                                                                    len(self.unik_tel),
                                                                                                    len(
                                                                                                        self.result_unik_tel))
        return result

    unik_tel = property()

    @unik_tel.setter
    def unik_tel(self, val):
        if val in self._unik_tel:
            return
        self._unik_tel.append(val)

    @unik_tel.getter
    def unik_tel(self):
        return self._unik_tel

    result_unik_tel = property()

    @result_unik_tel.setter
    def result_unik_tel(self, val):
        if val in self._result_unik_tel:
            return
        self._result_unik_tel.append(val)

    @result_unik_tel.getter
    def result_unik_tel(self):
        return self._result_unik_tel

        # --------------------------


def xlsx(workbook, td, name_sheet="лог звонков", plan_unik_result_tel=5):
    # Create a workbook and add a worksheet.
    # worksheet = workbook.add_worksheet(name_sheet)

    worksheet = workbook.get_worksheet_by_name(name_sheet)

    print(worksheet)

    # формат для выделения внимания
    format_red = workbook.add_format()
    format_red.set_bold()
    format_red.set_font_color('yellow')
    format_red.set_bg_color('red')
    format_red.set_align('vcenter')
    format_red.set_align('center')

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
    worksheet.write(0, 0, "Выгружено: {}".format(datetime.now()))
    worksheet.write(1, 0, "номер телефона", format_default)
    worksheet.write(1, 1, "ФИО МПП", format_default)
    worksheet.write(1, 2, "ФИО РГ", format_default)
    worksheet.write(1, 3, "Кол-во\nуникальных\nзвонков", format_default)
    worksheet.write(1, 4, "Кол-во\nрезультативных\nуникальных\nзвонков", format_default)
    worksheet.write(1, 5, "Плановое\nкол-во\nрезультативных\nуникальных\nзвонков\nв получасе", format_default)

    # координаты откуда будет заполнять таблицу данными
    row = 2
    col = 0
    for num_tel in td:
        kol_uniq_result_tel = len(td[num_tel].result_unik_tel)
        if kol_uniq_result_tel >= plan_unik_result_tel:
            format = format_default
        else:
            format = format_red
        worksheet.write(row, col, num_tel, format)
        worksheet.write(row, col + 1, td[num_tel].fio_manager, format)
        worksheet.write(row, col + 2, td[num_tel].fio_rg, format)
        worksheet.write(row, col + 3, len(td[num_tel].unik_tel), format)
        worksheet.write(row, col + 4, kol_uniq_result_tel, format)
        worksheet.write(row, col + 5, plan_unik_result_tel, format)
        row += 1


def get_inputdata_list(csv_filename, datas=None):
    """ чтение сырого лога звонков """
    with open(csv_filename) as csv_fd:
        # создаем объект csv.reader для чтения csv-файла
        reader = csv.reader(csv_fd, delimiter=';')
        # это наш список, который будем возвращать
        inputdata_list = BaseDataTable()
        # обрабатываем csv-файл построчно
        for row in reader:
            try:
                if row[InputData.ind_source_tel] in datas:  # отфильтровываем только нужные нам внутренние номера
                    # создаем и добавляем объект в inputdata_list
                    inputdata_list[row[InputData.ind_source_tel]] = InputData.from_tuple(row)
            except (ValueError, IndexError):
                # если данные некорректны, то игнорируем их
                pass
    return inputdata_list


def get_cfg_list(csv_filename):
    """ чтение конфиг файла - возвращает словарь , ключом которого является номер телефона"""
    with open(csv_filename) as csv_fd:
        # создаем объект csv.reader для чтения csv-файла
        reader = csv.reader(csv_fd, delimiter=';')
        # это наш список, который будем возвращать
        cfg_list = {}
        # обрабатываем csv-файл построчно
        for row in reader:
            try:
                # создаем и добавляем объект в inputdata_list
                cfg_list[row[TableData.ind_num_tel]] = TableData.from_tuple(row)
            except (ValueError, IndexError):
                # если данные некорректны, то игнорируем их
                pass
    return cfg_list


def calc(table_data, input_data, plan_result_sec, begin_date, begin_time, end_date, end_time):
    # plan_result_sec = 20  # плановая продолжительность результативного звонка (в сек)

    begin_date = begin_date.split("-")
    end_date = end_date.split("-")

    begin_time = begin_time.split(":")
    end_time = end_time.split(":")

    begin_year = int(begin_date[0])
    begin_month = int(begin_date[1])
    begin_day = int(begin_date[2])

    end_year = int(end_date[0])
    end_month = int(end_date[1])
    end_day = int(end_date[2])

    begin_time_hour = int(begin_time[0])
    begin_time_minute = int(begin_time[1])
    end_time_hour = int(end_time[0])
    end_time_minute = int(end_time[1])

    for num_tel in table_data:
        data_manager = input_data[num_tel]
        if data_manager == None:
            continue
        total_sec = 0  # общая продолжительность звонков (в сек)
        total_call = 0  # общее кол-во звоноков
        count_result_call = 0  # кол-во результативных звоноков
        count_unik_tel = 0  # кол-во уникальных телефонных номеров
        count_result_unik_tel = 0  # кол-во уникальных результативных звоноков
        result_sec = 0  # продолжительность результативных звонков (в сек)
        for el in data_manager:
            if (datetime(begin_year, begin_month, begin_day) <= el.datatimes) and (  # фильтрация по дате и времени
                        datetime(end_year, end_month, end_day, 23, 59) >= el.datatimes):
                if (time(begin_time_hour, begin_time_minute) <= el.datatimes.time()) and (
                            time(end_time_hour, end_time_minute, 59) >= el.datatimes.time()):
                    total_sec += int(el.secs)
                    total_call += 1
                    table_data[num_tel].unik_tel = el.tel_dest
                    if int(el.secs) >= plan_result_sec:
                        result_sec += int(el.secs)
                        table_data[num_tel].result_unik_tel = el.tel_dest

        table_data[num_tel].total_sec = total_sec
        table_data[num_tel].total_call = total_call


def run_log_zvonkov(begin_date, end_date, namefile_xlsx):
    # параметры программы
    plan_count_result_zvonok = 5
    plan_result_zvonok = 20  # продолжительность результативного звонка
    report_filename = "Reports.csv"  # файл куда сохраняются сырые данные лога звонков для последующей обработки
    # END параметры программы

    # скачивание данных в определенные даты из сайта данных
    b_d = begin_date.split("-")
    e_d = end_date.split("-")
    begyearmonth = "{}-{}".format(b_d[0], b_d[1])
    endyearmonth = "{}-{}".format(e_d[0], e_d[1])
    begday = b_d[2]
    endday = e_d[2]
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
    # END - скачивание данных в определенные даты из сайта данных

    try:
        table_data = get_cfg_list("list-num-tel.cfg")
    except FileNotFoundError:
        print("Файл конфига не обнаружен")
        return

    try:
        input_data = get_inputdata_list(report_filename, table_data)
    except FileNotFoundError:
        print("Файл сырого лога не обнаружен")
        return

    workbook = xlsxwriter.Workbook(namefile_xlsx)
    interval_time = (("13:00", "23:59"), ("13:00", "13:29"), ("13:30", "13:59"), ("14:00", "14:29"), ("14:30", "14:59"),
                     ("15:00", "15:29"), ("15:30", "15:59"), ("16:00", "23:59"))
    name_sheets = ("лог звонков(итоговый)", "время 9-00 до 9-30", "время 9-30 до 10-00", "время 10-00 до 10-30",
                   "время 10-30 до 11-00",
                   "время 11-00 до 11-30", "время 11-30 до 12-00", "время 12-00 до 23-59")

    # создаем листы в книге экселя
    for i in range(len(name_sheets)):
        workbook.add_worksheet(name_sheets[i])
        print(name_sheets[i])

    # блок расчета показателей в указанный промежуток времени
    calc(table_data, input_data, plan_result_zvonok, begin_date, interval_time[0][0], end_date, interval_time[0][1])
    xlsx(workbook, table_data, name_sheets[0], plan_count_result_zvonok * 5)
    for k in table_data:
        table_data[k].clear_calc()
    # END - блок расчета показателей в указанный промежуток времени

    for i in range(1, len(interval_time)):
        # блок расчета показателей в указанный промежуток времени
        calc(table_data, input_data, plan_result_zvonok, begin_date, interval_time[i][0], end_date, interval_time[i][1])
        xlsx(workbook, table_data, name_sheets[i], plan_count_result_zvonok)
        for k in table_data:
            table_data[k].clear_calc()
            # END - блок расчета показателей в указанный промежуток времени
    workbook.close()


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
    namefile = "logs-{} по {}.xlsx".format(begin_date, end_date)
    run_log_zvonkov(begin_date, end_date, namefile)
