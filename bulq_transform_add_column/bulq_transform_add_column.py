from datetime import datetime
from distutils.util import strtobool

import apache_beam as beam

from bulq.core.plugin_base import BulqTransformPlugin


class BulqTransformAddColumn(BulqTransformPlugin):
    VERSION = '0.0.1'
    

    def __init__(self, conf):
        self.columns = []
        for c in conf['columns']:
            entry = {}
            entry['name'] = c['name']
            entry['value'] = self.column_value(c)
            self.columns.append(entry)

    
    def column_value(self, column_entry_conf):
        col_type = column_entry_conf['type']
        col_value = column_entry_conf['value']
        if col_type == 'string':
            return col_value
        elif col_type == 'long':
            return int(col_value)
        elif col_type == 'double':
            return float(col_value)
        elif col_type == 'timestamp':
            return datetime.strptime(col_value, column_entry_conf['format'])
        elif col_type == 'boolean':
            return col_value
        else:
            raise Exception(f'invalid column type name: {col_type}')

    def add_columns(self, rec):
        for col in self.columns:
            rec[col['name']] = col['value']

        return rec

    def build(self, p):
        return (p
                | beam.Map(self.add_columns)
               )

    def setup(self):
        pass

