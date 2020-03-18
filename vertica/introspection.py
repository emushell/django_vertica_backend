from collections import namedtuple
from django.db.backends.base.introspection import BaseDatabaseIntrospection

TableInfo = namedtuple('TableInfo', ['name', 'type'])


class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps type codes to Django Field types.
    data_types_reverse = {
        5: 'BooleanField',
        117: 'BinaryField',
        6: 'IntegerField',
        7: 'FloatField',
        8: 'CharField',
        9: 'CharField',
        17: 'CharField',
        115: 'TextField',
        116: 'TextField',
        10: 'DateField',
        11: 'TimeField',
        15: 'TimeField',
        12: 'DateTimeField',
        13: 'DateTimeField',
        14: 'DurationField',
        16: 'DecimalField',
        20: 'UUIDField',
    }

    ignored_tables = []

    def get_table_list(self, cursor):
        """Return a list of table and view names in the current database."""
        query = ("""
        SELECT table_name, 't' 
          FROM v_catalog.tables
         union
        select table_name, 'v'
          from v_catalog.views
        """)
        cursor.execute(query)
        return [TableInfo(*row) for row in cursor.fetchall()]

    def get_sequences(self, cursor, table_name, table_fields=()):
        cursor.execute("""
        select seq.sequence_name,
               '' column_name
          from v_catalog.sequences seq
         where 1=1
           and a.table_name = %s
        """, [table_name])
        return [
            {'name': row[0], 'table': table_name, 'column': row[1]}
            for row in cursor.fetchall()
        ]

    def get_key_columns(self, cursor, table_name):
        pass

    def get_constraints(self, cursor, table_name):
        pass
