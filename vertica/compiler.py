from django.conf import settings
from django.db.models.sql.compiler import *

ENFORCE_CONSTRAINTS_VALIDATION = getattr(settings, "ENFORCE_CONSTRAINTS_VALIDATION", True)


class CustomSQLCompiler(SQLCompiler):
    def _execute_sql(self, result_type):
        return super().execute_sql(result_type)


class SQLUpdateCompiler(SQLUpdateCompiler, CustomSQLCompiler):
    def execute_sql(self, result_type):
        """
        Vertica dose not update by default rowcount property after updates.
        After update need to fetchone and get the returned value which is updated row count.
        """
        cursor = self._execute_sql(result_type)
        try:
            rows = cursor.fetchone()[0] if cursor else 0
            is_empty = cursor is None
        finally:
            if cursor:
                cursor.close()
        for query in self.query.get_related_updates():
            aux_rows = query.get_compiler(self.using).execute_sql(result_type)
            if is_empty and aux_rows:
                rows = aux_rows
                is_empty = False
        return rows
