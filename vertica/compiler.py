import datetime
from django.conf import settings
# from django.db.transaction import atomic
from django.db.models.sql.compiler import *

ENFORCE_CONSTRAINTS_VALIDATION = getattr(settings, "ENFORCE_CONSTRAINTS_VALIDATION", True)


# class SQLInsertCompiler(SQLInsertCompiler):
#
#     def execute_sql(self, return_id=False):
#         with atomic(using=self.using):
#             result = super(SQLInsertCompiler, self).execute_sql(return_id)
#
#             if ENFORCE_CONSTRAINTS_VALIDATION:
#                 self.connection.ops.validate_constraints(
#                     self.connection.currsor(),
#                     self.query.get_meta().db_table
#                 )
#
#             return result


class SQLUpdateCompiler(SQLUpdateCompiler):
    def as_sql(self):
        """
        For Vertica there was no quotation for datetime and string type variables and parameters.
        Needed to add custom logic to quote the the variables.
        """
        self.pre_sql_setup()
        if not self.query.values:
            return '', ()
        qn = self.quote_name_unless_alias
        values, update_params = [], []
        for field, model, val in self.query.values:
            if hasattr(val, 'resolve_expression'):
                val = val.resolve_expression(self.query, allow_joins=False, for_save=True)
                if val.contains_aggregate:
                    raise FieldError("Aggregate functions are not allowed in this query")
                if val.contains_over_clause:
                    raise FieldError('Window expressions are not allowed in this query.')
            elif hasattr(val, 'prepare_database_save'):
                if field.remote_field:
                    val = field.get_db_prep_save(
                        val.prepare_database_save(field),
                        connection=self.connection,
                    )
                else:
                    raise TypeError(
                        "Tried to update field %s with a model instance, %r. "
                        "Use a value compatible with %s."
                        % (field, val, field.__class__.__name__)
                    )
            else:
                # custom
                if isinstance(val, datetime.datetime) or isinstance(val, str):
                    val = field.get_db_prep_save(val, connection=self.connection)
                    val = "'" + val + "'"
                else:
                    val = field.get_db_prep_save(val, connection=self.connection)

            # Getting the placeholder for the field.
            if hasattr(field, 'get_placeholder'):
                placeholder = field.get_placeholder(val, self, self.connection)
            else:
                placeholder = '%s'
            name = field.column
            if hasattr(val, 'as_sql'):
                sql, params = self.compile(val)
                values.append('%s = %s' % (qn(name), placeholder % sql))
                update_params.extend(params)
            elif val is not None:
                values.append('%s = %s' % (qn(name), placeholder))
                update_params.append(val)
            else:
                values.append('%s = NULL' % qn(name))
        table = self.query.base_table
        result = [
            'UPDATE %s SET' % qn(table),
            ', '.join(values),
        ]
        where, params = self.compile(self.query.where)
        if where:
            result.append('WHERE %s' % where)

        # custom
        prms = []
        for param in params:
            if isinstance(param, datetime.datetime) or isinstance(param, str):
                prms.append("'" + param + "'")
            else:
                prms.append(param)
        params = prms
        return ' '.join(result), tuple(update_params + params)

    def execute_sql(self, result_type):
        """
        Vertica dose not update by default rowcount property after updates.
        After update need to fetchone and get the returned value which is updated row count.
        """
        cursor = self.connection.cursor()
        cursor.execute(str(self.query))
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
