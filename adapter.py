from django.db.models import Model
from django.core.exceptions import ObjectDoesNotExist

from typing import Type, Any, List, Dict
from datetime import date
from collections import Counter

class AdapterStaging(object):
    """ Add changes from Excel file to be staged.
    Commit changes to models and DB.
    """
    EXEMPTED_COLUMNS = []

    PRECOMMIT_DELETE_LAMBDA = 'precommit_delete'
    POSTCOMMIT_DELETE_LAMBDA = 'postcommit_delete'

    PRECOMMIT_UPDATE_LAMBDA = 'precommit_update'
    POSTCOMMIT_UPDATE_LAMBDA = 'postcommit_update'

    def __init__(self):
        self.update_mapping = {}
        self.delete_mapping = {}

        self.commit_lambdas = {}
        self.commit_lambdas_data = {}

        super().__init__()

    def _key_index(self, model, pk):
        for idx, m in enumerate(self.update_mapping[model]):
            if m[0] == pk:
                return idx

        return -1

    def commit(self) -> int:
        """Commits the current changes applied to this updater
        
        :return: The amount of rows modified during this commit
        :rtype: int
        """
        rows_updated = 0

        for model, update_vals in self.update_mapping.items():
            for pk, column_updates in update_vals:
                try:
                    row = model.objects.get(pk=pk)
                    columns_updated = 0
                    for key, value in column_updates.items():
                        # Check if this key needs to be deleted
                        if key is 'delete_tag' and column_updates[key] == 'X':
                            self._run_lambdas(self.PRECOMMIT_DELETE_LAMBDA, pk, row)
                            row.delete()
                            self._run_lambdas(self.POSTCOMMIT_DELETE_LAMBDA, pk, row)
                            rows_updated += 1
                        # Check if values has changed
                        elif getattr(row, key) != column_updates[key]:
                            # Don't set values for exempted columns
                            if key not in EXEMPTED_COLUMNS:
                                setattr(row, key, value)
                            columns_updated += 1
                
                    if columns_updated > 0:
                        self._run_lambdas(self.PRECOMMIT_UPDATE_LAMBDA, pk, row)
                        row.save()
                        self._run_lambdas(self.POSTCOMMIT_UPDATE_LAMBDA, pk, row)
                        rows_updated += 1
                except ObjectDoesNotExist:
                    pass

        return rows_updated

    def add(self, model: Model, pk: str, **kwargs):
        """Adds a future update to a specific column in a model
        
        :param model: The `django.db.models.Model` object to apply the update
        :type model: django.db.models.Model
        :param pk: The primary key this update should apply to
        :type pk: int
        """
        # Add model type to update mapping
        if model not in self.update_mapping:
            self.update_mapping[model] = [
                (pk, kwargs)
            ]
        else:
            key_index = self._key_index(model, pk)
            if key_index >= 0:
                self.update_mapping[model][key_index][1].update(**kwargs)
            else:
                self.update_mapping[model].append(
                    (pk, kwargs)
                )
    
    def delete(self, model: Model, pk: str, **kwargs):
        """Stages a future deletion to a specific row of a model
        
        :param model: The `django.db.models.Model` object to apply the update
        :type model: django.db.models.Model
        :param pk: The primary key this update should apply to
        :type pk: int
        """
        # Add model type to update mapping
        if model not in self.update_mapping:
            self.update_mapping[model] = [
                (pk, {'delete_tag': 'X'})
            ]
        else:
            self.update_mapping[model].append(
                (pk, {'delete_tag': 'X'})
            )

    def add_commit_runnable(self, runnable_type: str, pk: str, runnable):
        """Adds a lambda to be executed after `commit()` has been called on this object

        :param runnable_type: The time in the commit (pre/post update/delete) at which to run the passed lambda
        :param pk: The primary key of the row the runnable will be executed for after a commit
        :param runnable: A `lambda` taking in a `Model` object
        :type runnable: lambda
        """
        if runnable_type not in self.commit_lambdas:
            self.commit_lambdas[runnable_type] = {}

        if pk not in self.commit_lambdas[runnable_type]:
            self.commit_lambdas[runnable_type][pk] = []
            self.commit_lambdas_data[pk] = {}

        self.commit_lambdas[runnable_type][pk].append(runnable)

    def has_commit_runnable(self, runnable_type: str, pk: str):
        """Checks to see if a runnable has already been scheduled for the specific type and DB row

        :param runnable_type: The type of lambda to check
        :type runnable_type: str
        :param pk: The database row
        :type pk: str
        :return: True if at least one runnable has been scheduled
        :rtype: bool
        """
        return runnable_type in self.commit_lambdas and pk in self.commit_lambdas[runnable_type]

    def _run_lambdas(self, runnable_type: str, pk: str, row: Model):
        """Runs any existing lambdas for the specific type and primary key

        :param runnable_type: The type of lambdas to run
        :type runnable_type: str
        :param pk: The primary key of the row
        :type pk: str
        :param row: The Model object containing the row information
        :type row: Model
        """
        if runnable_type in self.commit_lambdas and pk in self.commit_lambdas[runnable_type]:
            for runnable in self.commit_lambdas[runnable_type][pk]:
                result = runnable(self.commit_lambdas_data[pk], row)
                self.commit_lambdas_data[pk] = result if result else self.commit_lambdas_data[pk]


class Column(object):
    def __init__(self, header, extractor=None, inserter=None):
        self.header = header
        self.extractor = extractor
        self.inserter = inserter

        super().__init__()

    def is_modifiable(self) -> bool:
        """Determines whether or not this column can be modified
        
        :return: `True` if possible, `False` if not
        :rtype: bool
        """
        return self.inserter is not None


class BaseAdapter(object):
    model: Type[Model] = Model
    columns: Dict[str, Column] = {}

    @classmethod
    def _get_column(cls, column: str):
        return cls.columns[column]

    @classmethod
    def _get_header_key(cls, header_name: str):
        if header_name.endswith('*'):
            header_name = header_name[:-1]
        for key, column_def in cls.columns.items():
            if column_def.header == header_name:
                return key
        
        raise ValueError('Header is not defined in table')

    @classmethod
    def all_headers(cls) -> List[str]:
        """Returns all of the headers available in this table
        
        :return: The list of header names
        :rtype: List[str]
        """
        return [cd.header for cd in cls.columns.values()]

    @classmethod
    def all_display_headers(cls) -> List[str]:
        """Returns all of the headers available in this table but appends a * to headers that may be modified when importing values
        
        :return: The list of headers
        :rtype: List[str]
        """
        return ['{0}*'.format(cd.header) if cd.is_modifiable() else cd.header for cd in cls.columns.values()]

    @classmethod
    def all_columns(cls) -> List[str]:
        """Returns the list of database columns available in this table
        
        :return: The list of column names
        :rtype: List[str]
        """
        return list(cls.columns.keys())
    @classmethod
    def extract(cls, row, column: str) -> Any:
        """Extracts the value of column from a specific database row using the defined extractor
        
        :param row: The row from the database containing the data
        :type row: 
        :param column: The name of the column to extract
        :type column: str
        :return: The value extracted from the row
        :rtype: any
        """
        column_def = cls._get_column(column)
        return column_def.extractor(row) if column_def.extractor is not None else getattr(row, column)

    @classmethod
    def insert(cls, table_updater: AdapterStaging, row, column: str, value: Any):
        """Queues a future update to a value of a row using a `AdapterStaging` object
        
        :param table_updater: The table update object to write data into
        :type table_updater: AdapterStaging
        :param row: The database row 
        :param column: The column that is being modified
        :type column: str
        :param value: The value to write into the database row
        :type value: Any
        """
        column_def = cls._get_column(column)
        if column_def.is_modifiable():
            column_def.inserter(table_updater, row, value)
        elif column == 'delete_tag' or column == 'copy_tag':
            column_def.inserter(table_updater, row, value)
    
    @classmethod
    def header_name(cls, column: str) -> str:
        """Retrieves the header name using a column name
        
        :param column: The column to look up
        :type column: str
        :return: The header name
        :rtype: str
        """
        return cls._get_column(column).header

    @classmethod
    def column_name(cls, header_name: str) -> str:
        """Retrieves the column name using a header
        
        :param header_name: The header name to look up
        :type header_name: str
        :return: The column name
        :rtype: str
        """
        return cls._get_header_key(header_name)