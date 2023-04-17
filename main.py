"""Основной модуль программы для обработки данных файлов, логирования и обертки БД"""
import datetime
import os

import warnings
from datetime import datetime
import sqlite3
import numpy as np
import pandas as pd


# Класс работы с логами.
class ConsoleLogFile:
    """
        Класс работы с сообщения в консоль/файл (логирование)
    """
    _dtf_format = '[%Y-%m-%d %H:%M:%S]'

    def __init__(self):
        self._enable_log_file = False
        self._logfile_name = self.change_file_ext(__file__, "log")

    @property
    def write_logfile(self):
        """
            Признак записи в файл.
        :return: Bool
        """
        return self._enable_log_file

    @write_logfile.setter
    def write_logfile(self, value: bool):
        """
            Включить запись в файл.
        :param value: Bool
        :return: Nothing
        """
        self._enable_log_file = value

    @classmethod
    def change_file_ext(cls, file_name: str, new_ext: str):
        """
            Меняет расширение файла на заданное.
        :param file_name: Имя файла. (test.txt)
        :param new_ext: Новое расширение. (log)
        :return: Имя файла с расширением. (test.log)
        """
        _file_name = file_name.split('.')[0]
        return f'{_file_name}{new_ext}' if '.' in new_ext else f'{_file_name}.{new_ext}'

    def log_data(self, line):
        """
            Запись строк в логи
        :param line: Строка для записи.
        :return:
        """
        line = f'{datetime.now().strftime(self._dtf_format)} - {line}'
        print(line)
        if self._enable_log_file:
            with open(f'{self._logfile_name}', 'a', encoding='UTF16') as file:
                file.write(line + chr(13))


class DataFileParser(ConsoleLogFile):
    """
        Класс осуществляющий обработку файлов из директории import.
    """
    __slots__ = ['_files_list']
    # Поддерживаемые анализатором форматы excel файлов.
    # Сравниваются полные lowercase совпадения колонок страницы excel файла.
    # Если нам требуется какая-то колонка, которая не имеет имя, то ее указываем как None
    _supported_formats = {
        "class_gtins": {
            "cols": [
                # Номер колонки, имя колонки, строго требовать наличие значений в строке.
                {"id": 1, "val": "GTIN", "strict": True},
                {"id": 2, "val": "Наименование товара", "strict": True},
                {"id": 5, "val": "ИНН произв.", "strict": True},
                {"id": 6, "val": "ТН ВЭД", "strict": True},
                {"id": 8, "val": "Артикул", "strict": True}],
            # Строка откуда начинаются данные.
            "offset": 0,
            # Этот формат - сохраняется в таблицу.
            "table": "gtin_data",
        },
        "class_order": {
            "cols": [
                {"id": 0, "val": "ИНН", "strict": True},
                {"id": 1, "val": None, "strict": False},
                {"id": 2, "val": "ФИО", "strict": True},
                # Торговые марки.
                {"id": 3, "val": None, "strict": True},
                {"id": 4, "val": None, "strict": True},
                {"id": 5, "val": "ЗПОЛНЯТЬ ТОЛЬКО ЖЕЛТЫЕ ЯЧЕЙКИ!!", "strict": False},
                {"id": 6, "val": None, "strict": True},
                {"id": 7, "val": None, "strict": True},
                {"id": 8, "val": None, "strict": True},
                {"id": 9, "val": None, "strict": True},
                {"id": 10, "val": None, "strict": True},
                {"id": 11, "val": None, "strict": True},
                {"id": 12, "val": None, "strict": True}],
            # Строка с которой начинаются реальные данные у этого формата.
            "offset": 2
        }
    }

    def __init__(self):
        super().__init__()
        self._path = os.getcwd() + '\\'
        self._import_path = self._path + 'import\\'
        if not os.path.isdir(self._import_path):
            os.mkdir(self._import_path)
        self._files_list = {_k: [] for _k in self._supported_formats}

    @property
    def collected_data(self):
        """
            Свойство, предоставляющее отобранные данные, которые прошли проверку шаблонизатора.
        :return: Dict
        """
        return self._files_list

    @classmethod
    def detect_data_format(cls, col_array: list = None, data_format: str = None):
        """
            Проверяет массив numpy на соответствие формату данных.
        :param data_format: Проверяемый формат
        :param col_array: Входящий массив
        :return: Bool
        """
        success = 0
        format_cols = cls._supported_formats[data_format]['cols']
        for _item in format_cols:
            if (str(_item['val']).lower() == str(col_array[_item['id']]).lower()) \
                    or _item['val'] is None:
                success += 1
            else:
                break
            if success == len(format_cols):
                return True
        return False

    @classmethod
    def create_list(cls, in_list: list = None, data_format: str = ''):
        """
            Генерирует список по шаблону из файла с данными.
        :param in_list: Входящий массив numpy
        :param data_format: Тип данных (имя шаблона)
        :return: List
        """
        # Оставляем только сравниваемые колонки в памяти, остальные - нас не интересуют.
        ret_val = []
        for _k in in_list[(cls._supported_formats.get(data_format, {}).get('offset', 0))::]:
            success = 0
            line = np.array(_k).take(
                [_id.get('id') for _id in cls._supported_formats.get(data_format, {}).get('cols')])
            for _idx, _strict in enumerate(
                    [_id.get('strict') for
                     _id in cls._supported_formats.get(data_format, {}).get('cols')]):
                # Проверим строчку данных на соответствие наличия данных
                success += 1 if line[_idx] != 'nan' else 1 if not _strict else 0
            if success == len(line):
                ret_val.append(tuple(line.tolist()))
        return ret_val

    def scan_import_files(self):
        """
            Поиск и анализ всех файлов директории import.
        :return: Nothing
        """

        def build_val_list(_data_format: str):
            return "?, ".join(["" for _ in self._supported_formats[_data_format]["cols"]]) + "?"

        for file in os.listdir(self._import_path):
            _full_path = f'{self._import_path}{file}'
            self.log_data(f'Найден новый файл импорта: {_full_path}')
            try:
                with warnings.catch_warnings(record=True):
                    warnings.simplefilter("always")
                    with pd.ExcelFile(_full_path) as xls:
                        for sheet in xls.sheet_names:
                            for _format in self._supported_formats:
                                if self.detect_data_format(xls.parse(sheet).columns.tolist(), _format):
                                    item = {
                                        "file": _full_path,
                                        "sheet": sheet,
                                        "data": self.create_list(xls.parse(sheet).values.tolist(),
                                                                 _format)
                                    }
                                    # Данный тип данных нужно загрузить напрямую в таблицу:
                                    format_table = self._supported_formats[_format].get('table')
                                    if format_table:
                                        item['sql'] = \
                                            f'' \
                                            f'insert or ignore into {format_table} ' \
                                            f'values ({build_val_list(_format)})'
                                    self._files_list[_format].append(item)
                                    self.log_data(
                                        f'{file} [{sheet}] - определен как: {_format}, '
                                        f'данных: {len(self._files_list[_format][-1]["data"])} шт.')
                                else:
                                    self.log_data(f'{file} [{sheet}] - {_format}, пропускаем.')
            except PermissionError:
                self.log_data(f'Ошибка доступа к файлу {_full_path}, '
                              f'файл открыт другой программой?')
            except Exception as ex:
                self.log_data(f'Ошибка импорта файла ({_full_path}): {ex}')


class DatabaseWrapper(ConsoleLogFile):
    """
        Класс работы с БД SQL.
    """
    __slots__ = []

    def __init__(self):
        super().__init__()
        self._database_file = self.change_file_ext(__file__, "db")
        self._connection = sqlite3.connect(self._database_file)
        self._initial_db()

    @property
    def cursor(self):
        """
            Курсор для работы с БД SQL.
        :return: Nothing
        """
        return self._connection.cursor()

    def execute(self, _sql: str, params: list = None):
        """
            Выполнение SQL запроса Execute
        :param _sql: Тело запроса.
        :param params: Параметры (список кортежей [(p1, p2), (p3, p4)]).
        :return: Nothing
        """
        self._sql_execute(_sql, params)

    def _sql_execute_script(self, _sql: str):
        try:
            self.cursor.executescript(_sql)
        except Exception as exc:
            self.log_data(f'SQL {__name__}: {exc}')

    def _sql_execute(self, _sql: str, params: list = None):
        try:
            if params:
                self.cursor.executemany(_sql, params)
            else:
                self.cursor.execute(_sql)
            self.cursor.connection.commit()
        except Exception as exc:
            self.log_data(f'Ошибка SQL: {exc}')

    def _initial_db(self):
        _sql = "create table if not exists gtin_data " \
               "(" \
               "gtin str(16) primary key, " \
               "name str(256) not null, " \
               "inn_producer str(16) not null default '', " \
               "tn_code str(32) not null default '', " \
               "ware_code str(32) not null default  ''" \
               ");" \
               "create index if not exists gtin_data_name_index on gtin_data (name);"
        self._sql_execute_script(_sql)


# Demo call
parse = DataFileParser()
parse.write_logfile = False
parse.scan_import_files()
base = DatabaseWrapper()
for k in parse.collected_data.keys():
    for i in parse.collected_data[k]:
        sql = i.get('sql')
        if sql:
            base.execute(sql, i['data'])
# print(parse.collected_data)
