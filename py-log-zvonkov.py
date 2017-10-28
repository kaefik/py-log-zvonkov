import csv

class InputData:
    """ структура входящих данных лога звонков"""

    # индексы полей, которые соответствуют колонкам в исходном csv-файле
    ind_datetime = 0   # дата и время начала звонка
    ind_source_tel = 1 # источник звонка (внутренний номер)
    ind_dest_tel = 2 # номер телефона куда звонили
    ind_secs = 10   # продолжительность звонка в секундах

    def __init__(self,datatimes,tel_source,tel_dest,secs):
        self.datatimes = datatimes
        self.tel_source = tel_source
        self.tel_dest = tel_dest
        self.secs = secs

    def __str__(self):
        result = "Дата: {}\nИсточник: {}\nЦель: {}\nПродолжительность: {}\n".format(self.datatimes,self.tel_source,self.tel_dest,self.secs)
        return result

    @classmethod
    def from_tuple(cls, row):
        """ Метод для создания экземпляра InputData
            из строки csv-файла"""
        return cls(
            row[cls.ind_datetime],
            row[cls.ind_source_tel],
            row[cls.ind_dest_tel],
            row[cls.ind_secs],
        )


class TableData:
    """ итоговые данные"""

    # индексы полей, которые соответствуют колонкам в исходном csv-файле
    ind_num_tel = 0  # номер телефона МПП
    ind_fio_manager = 1  # ФИО МПП
    ind_fio_rg = 2  # ФИО РГ

    def __init__(self,fio_manager,fio_rg):
        # self.num_tel = num_tel
        self.fio_manager = fio_manager
        self.fio_rg = fio_rg
        self.total_sec = 0         # общая продолжительность звонков (в сек)
        self.count_unik_tel = 0    # кол-во уникальных телефонных номеров
        self.total_call = 0        # общее кол-во звоноков
        self.count_result_call = 0 # кол-во результативных звоноков
        self.result_sec = 0        # продолжительность результативных звонков (в сек)

    @classmethod
    def from_tuple(cls, row):
        """ Метод для создания экземпляра TableData
            из строки csv-файла"""
        return cls(
            # row[cls.ind_num_tel],
            row[cls.ind_fio_manager],
            row[cls.ind_fio_rg],
        )

    def __str__(self):
        result = "ФИО МПП: {}\nФИО РГ: {}\n".format(self.fio_manager,self.fio_rg)
        return result




# --------------------------

def get_inputdata_list(csv_filename):
    """ чтение сырого лога звонков """
    with open(csv_filename) as csv_fd:
        # создаем объект csv.reader для чтения csv-файла
        reader = csv.reader(csv_fd, delimiter=';')

        # это наш список, который будем возвращать
        inputdata_list = []

        # обрабатываем csv-файл построчно
        for row in reader:
            try:
                # создаем и добавляем объект в inputdata_list
                inputdata_list.append(InputData.from_tuple(row))
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


def run_log_zvonkov(*args):

    # параметры программы
    result_sec = 20  # продолжительность результативного звонка (в сек)
    # END параметры программы

    try:
        input_data = get_inputdata_list("Report.csv")
    except FileNotFoundError:
        print("Файл сырого лога не обнаружен")
        return

    print(input_data)

    try:
        table_data = get_cfg_list("list-num-tel.cfg")
    except FileNotFoundError:
        print("Файл конфига не обнаружен")
        return


    for k in table_data:
        print(k)
        print(table_data[k])



if __name__ == "__main__":
    run_log_zvonkov()