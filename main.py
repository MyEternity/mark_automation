# Initial
import datetime
import os

import numpy as np
import pandas as pd
import warnings
from datetime import datetime


# Класс работы с логами.
class ConsoleLogFile:
    _dtf_format = '[%Y-%m-%d %H:%M:%S]'

    def __init__(self):
        self._enable_log_file = False

    @property
    def write_logfile(self):
        return self._enable_log_file

    @write_logfile.setter
    def write_logfile(self, value: bool):
        self._enable_log_file = value

    def log_data(self, line):
        line = f'{datetime.now().strftime(self._dtf_format)} - {line}'
        print(line)
        if self._enable_log_file:
            with open(f'{os.path.basename(__file__)}.log', 'a', encoding='UTF16') as f:
                f.write(line + chr(13))


class DataFileParser(ConsoleLogFile):
    # Поддерживаемые анализатором форматы excel файлов.
    # Сравниваются полные lowercase совпадения колонок страницы excel файла.
    # Если нам требуется какая-то колонка, которая не имеет имя, то ее указываем как None
    _supported_formats = {
        "ts_gtin_data": {
            "cols": [
                {"id": 1, "val": "GTIN", "strict": True},
                {"id": 2, "val": "Наименование товара", "strict": True},
                {"id": 6, "val": "ТН ВЭД", "strict": True},
                {"id": 8, "val": "Артикул", "strict": True}],
            "offset": 0
        },
        "client_order": {
            "cols": [
                {"id": 0, "val": "ИНН", "strict": True},
                {"id": 1, "val": None, "strict": True},
                {"id": 2, "val": "ФИО", "strict": True},
                {"id": 4, "val": "Называем файл Своим ФИО-ИНН", "strict": False},
                {"id": 5, "val": "ЗПОЛНЯТЬ ТОЛЬКО ЖЕЛТЫЕ ЯЧЕЙКИ!!", "strict": False}],
            # Строка с которой начинаются реальные данные у этого формата.
            "offset": 2
        }
    }

    def __init__(self):
        self._path = os.getcwd() + '\\'
        self._import_path = self._path + 'import\\'
        self._files_list = {k: [] for k in self._supported_formats.keys()}

    @property
    def collected_data(self):
        return self._files_list

    @classmethod
    def detect_data_format(cls, col_array: list = None):
        # Проверка данных на формат - путем сравнения переданных данных страницы с эталоном.
        for k in cls._supported_formats.keys():
            success = 0
            format_cols = cls._supported_formats[k]['cols']
            for c in format_cols:
                if (str(c['val']).lower() == str(col_array[c['id']]).lower()) or c['val'] is None:
                    success += 1
                else:
                    break
                if success == len(format_cols):
                    return k, None
        return 'Неизвестный формат данных.', True

    @classmethod
    def create_list(cls, in_list: list = None, data_format: str = ''):
        # Оставляем только сравниваемые колонки в памяти, остальные - нас не интересуют.
        ret_val = []
        for k in in_list[(cls._supported_formats.get(data_format, {}).get('offset', 0))::]:
            success = 0
            line = np.array(k).take([k.get('id') for k in cls._supported_formats.get(data_format, {}).get('cols')])
            for i, l in enumerate([k.get('strict') for k in cls._supported_formats.get(data_format, {}).get('cols')]):
                # Проверим строчку данных на соответствие наличия данных
                success += 1 if line[i] != 'nan' else 1 if not l else 0
            if success == len(line):
                ret_val.append(line.tolist())
        return ret_val

    def scan_import_files(self):
        for file in os.listdir(self._import_path):
            _full_path = f'{self._import_path}{file}'
            self.log_data(f'Найден новый файл импорта: {_full_path}')
            try:
                with warnings.catch_warnings(record=True):
                    warnings.simplefilter("always")
                    with pd.ExcelFile(_full_path) as xls:
                        for sheet in xls.sheet_names:
                            data_format, error_code = self.detect_data_format(xls.parse(sheet).columns.tolist())
                            if error_code:
                                self.log_data(f'{file} [{sheet}] - {data_format}, пропускаем.')
                                break
                            else:
                                self._files_list[data_format].append({
                                    "file": _full_path,
                                    "sheet": sheet,
                                    "data": self.create_list(xls.parse(sheet).values.tolist(), data_format)
                                })
                                self.log_data(
                                    f'{file} [{sheet}] - определен как: {data_format}, '
                                    f'данных: {len(self._files_list[data_format][-1]["data"])} шт.')
            except Exception as exc:
                self.log_data(f'Ошибка импорта файла ({_full_path}): {exc}')


# Demo call
parse = DataFileParser()
parse.write_logfile = True
parse.scan_import_files()
pass
