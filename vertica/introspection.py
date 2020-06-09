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
        """
        Return a list of (column_name, referenced_table_name, referenced_column_name)
        for all key columns in the given table.
        """
        key_columns = []
        cursor.execute("""
                    select fk.column_name,
                           fk.reference_table_name,
                           fk.reference_column_name
                      from v_catalog.foreign_keys fk
                     where 1=1
                       and fk.table_name = %s""", [table_name])
        key_columns.extend(cursor.fetchall())
        return key_columns

    def get_constraints(self, cursor, table_name):
        """
        Retrieve any constraints or keys (unique, pk, fk, check, index) across
        one or more columns.
        Vertica dose not have indexes
        """
        constraints = {}
        cursor.execute("""
        select tc.constraint_name,
               tc.constraint_type
          from v_catalog.table_constraints tc 
         where 1=1
           and tc.table_name = %s
        """, [table_name])

        for constraint, constraint_type in cursor.fetchall():
            constraints[constraint] = {
                "primary_key": constraint_type == "p",
                "unique": constraint_type in ["p", "u"],
                "check": constraint_type == "c",
                "index": False,
            }

            cursor.execute("""
            select cc.column_name ,
                   cc.reference_table_name || '.' || cc.reference_column_name used_cols
              from constraint_columns cc
             where 1=1
               and cc.table_name = %s
               and cc.constraint_name = %s
            """, [table_name, constraint])

            columns = []
            used_columns = []
            for column, used_cols in cursor.fetchall():
                columns.append(column)
                if constraint_type == "f":
                    used_columns.append(tuple(used_cols.split(".", 1)))

            constraints[constraint]["columns"] = columns
            constraints[constraint]["foreign_key"] = used_columns if constraint_type == "f" else None

        return constraints
