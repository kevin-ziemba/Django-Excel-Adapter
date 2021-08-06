from io import StringIO, BytesIO
from datetime import datetime
from typing import Dict

from .definitions import BaseAdapter, GenericInfo
import xlsxwriter


class Exporter(object):
    """Responsible for converting database rows into a specific file format
    
    :param table_definition: The table definition used for extract table values
    :type table_definition: BaseAdapter
    """
    def __init__(self, table_definition: BaseAdapter):
        super().__init__()
        self._table_definition = table_definition

    def export(self, data):
        """Handles exporting DB rows into a specific file format
        
        :param data: The queryset to export
        :type data: QuerySet
        """
        pass

    def _get_headers(self):
        return self._table_definition.all_display_headers()

    def _get_row_value(self, row, key):
        excepted_columns = ['delete_tag', 'copy_tag']
        try:
            return None if key in excepted_columns else self._table_definition.extract(row, key)
        except Exception as e:
            return 'ERROR'


class CSVExporter(Exporter):
    def export(self, data):
        import csv

        # The CSV file will be written to memory
        csv_string = StringIO()

        writer = csv.writer(csv_string)

        # Write the header info rows
        for row in GenericInfo.info_rows:
            writer.writerow(row)

        # Write the headers
        writer.writerow(self._get_headers())

        # Write all of the table data
        for row in data:
            row_data = []
            for key in self._table_definition.all_columns():
                value = self._get_row_value(row, key)
                row_data.append(value)

            writer.writerow(row_data)

        return csv_string


class ExcelExporter(Exporter):
    _type_translator = {
        str: lambda w: w.write_string,
        int: lambda w: w.write_number,
        float: lambda w: w.write_number,
        datetime: lambda w: w.write_datetime,
        bool: lambda w: w.write_boolean
    }

    def _write(self, worksheet, row, col, value):
        # Get the appropriate write method to write to the worksheet
        write_func = self._type_translator[type(value)](worksheet) if type(
            value) in self._type_translator else worksheet.write
        write_func(row, col, value)

    def export(self, data):
        # All of the Excel data will be written to memory
        excel_bytes = BytesIO()

        writer = xlsxwriter.Workbook(excel_bytes, {'in_memory': True})
        worksheet = writer.add_worksheet(GenericInfo.worksheet)
        worksheet.set_column(0, 0, options={'hidden': 1})

        headers = list(self._get_headers())

        cur_row = 0

        # Write all of the header info rows first
        for row in GenericInfo.info_rows:
            for col in range(len(row)):
                self._write(worksheet, cur_row, col, row[col])
            cur_row += 1

        # Create a table with headers for all exported data to be saved into.
        # Header cells must be created at the same as the table to avoid formatting error.
        table_data = {'columns': []}
        info_rows_count = len(GenericInfo.info_rows)
        for i in range(0, len(headers)):
            table_data['columns'].append({'header': headers[i]})
            self._write(worksheet, cur_row, i, headers[i])
        worksheet.add_table(info_rows_count, 0, len(data) + info_rows_count, len(headers)-1, table_data)
        cur_row += 1

        header_keys = list(self._table_definition.all_columns())

        # Extract and write all of the table values to the Excel document
        for row in data:
            for i in range(0, len(header_keys)):
                key = header_keys[i]
                self._write(worksheet, cur_row, i, self._get_row_value(row, key))

            cur_row += 1

        writer.close()

        return excel_bytes
