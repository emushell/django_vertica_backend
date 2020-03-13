from collections import namedtuple
from django.db.backends.base.introspection import BaseDatabaseIntrospection

TableInfo = namedtuple('TableInfo', ['name', 'type'])


class DatabaseIntrospection(BaseDatabaseIntrospection):

    def get_table_list(self, cursor):
        """Return a list of table and view names in the current database."""
        query = ("SELECT table_name, 't' FROM v_catalog.tables")
        cursor.execute(query)
        return [TableInfo(*row) for row in cursor.fetchall()]

    def get_sequences(self, cursor, table_name, table_fields=()):
        pass

    def get_key_columns(self, cursor, table_name):
        pass

    def get_constraints(self, cursor, table_name):
        pass
