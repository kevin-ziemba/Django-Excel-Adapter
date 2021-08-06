import csv

from io import BytesIO, StringIO
from xlrd import open_workbook

from .definitions import BaseAdapter, AdapterStaging, GenericInfo


class Importer(object):
    """A class for handling data imports that may be committed back to the database.

    :param BaseAdapter table_definition: The specific model adapter that will be used to import data
    :param parent_model: If the model has a foreign key pointing to it, this is the model datatype
    of that model.
    :param destination_object_id: Extending the use case of parent_model, this is the int DB id
    of the row pointning to this object. Assumed 1 to many relationship, and this object is on the many side.
    """
    parent_model = None
    destination_object_id = None

    def __init__(self, table_definition: BaseAdapter):
        self._table_definition = table_definition
        self._table_updater = AdapterStaging()
        super().__init__()

    def import_data(self, data: BytesIO) -> int:
        """Handles importing data back into the database. The `commit()` method will be called at the end of this process.
        
        :param BytesIO data: The raw data received on the server
        """
        return self.commit()

    def commit(self) -> int:
        """Commits the current changes made to the local `AdapterStaging` object.
        
        :returns: The amount of rows updated in the database
        """
        return self._table_updater.commit()

    def _get_column(self, header_name):
        return self._table_definition.column_name(header_name)

    def _update_column(self, row, column, new_value):
        self._table_definition.insert(self._table_updater, row, column, new_value)


class CSVImporter(Importer):
    def import_data(self, data: BytesIO) -> int:
        data_str = data.read().decode('UTF-8')

        # The CSV reader is expecting a file-like string
        data_io = StringIO(data_str)
        data_io.seek(0)

        reader = csv.reader(data_io, delimiter=',')

        header_count = len(GenericInfo.info_rows)
        key_lookup = {}
        delete_tag_column = -1

        for row_count, row in enumerate(reader):
            # Skip info rows
            if row_count < header_count:
                row_count += 1
                continue

            # Extract all the headers and convert them to column keys
            if row_count == header_count:
                for idx, column in enumerate(row):
                    try:
                        key_lookup[self._table_definition.column_name(column)] = idx
                    except:
                        pass
                continue
            
            # Check if row is marked for deletion before reading for any other columns
            # If marked for deletion, skip checking the rest of the row for updates.
            if 'delete_tag' in key_lookup and 'X' in row[key_lookup['delete_tag']].upper():
                dbid = int(row[key_lookup['id']])
                self._table_definition.insert(self._table_updater, dbid, 'delete_tag', 'X')
                continue

            # Iterate over all of the headers and store the values in the table updater
            for key, idx in key_lookup.items():
                if key == 'id':
                    db_id = int(row[idx])
                elif key == 'delete_tag':
                    continue
                else:
                    self._table_definition.insert(self._table_updater, db_id, key, row[idx])

        return super().import_data(data)


class ExcelImporter(Importer):
    def import_data(self, data: BytesIO) -> int:
        file_contents = data.read()

        with open_workbook(file_contents=file_contents) as wb:
            sheet = wb.sheet_by_name(GenericInfo.worksheet)

            if sheet:
                info_row_count = len(GenericInfo.info_rows)
                key_lookup = {}
                
                for row in range(sheet.nrows):
                    # Skip info rows
                    if row < info_row_count:
                        continue

                    # Convert all headers into column keys
                    # Save index of deletion column for quick reference
                    if row == info_row_count:
                        for column in range(sheet.ncols):
                            try:
                                key_lookup[self._table_definition.column_name(sheet.cell(row, column).value)] = column
                            except:
                                pass
                        continue
                    
                    # Check if row is marked for deletion before reading for any other columns
                    # If marked for deletion, skip checking the rest of the row for updates.
                    excepted_columns = ['delete_tag', 'copy_tag']
                    for excepted_column in excepted_columns:
                        if excepted_column in key_lookup and 'X' in sheet.cell(row, key_lookup[excepted_column]).value.upper():
                            dbid = int(sheet.cell(row, key_lookup['id']).value)
                            self._table_definition.insert(self._table_updater, dbid, excepted_column, self.destination_object_id)
                            continue
                    
                    # Write all column information into the table updater
                    dbid = -1
                    for key, column in key_lookup.items():
                        if key == 'id':
                            dbid = int(sheet.cell(row, column).value)
                        elif key in excepted_columns:
                            continue
                        else:
                            self._table_definition.insert(self._table_updater, dbid, key, sheet.cell(row, column).value)

        return super().import_data(data)
