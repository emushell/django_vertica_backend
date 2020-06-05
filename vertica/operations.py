from django.db.backends.base.operations import BaseDatabaseOperations
from django.conf import settings


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "vertica.compiler"

    def max_name_length(self):
        return 128

    def quote_name(self, name):
        if name.startswith('"') and name.endswith('"'):
            return name
        return '"%s"' % name

    def last_insert_id(self, cursor, table_name, pk_name):
        cursor.execute("SELECT LAST_INSERT_ID()")
        return cursor.fetchone()[0]

    def date_extract_sql(self, lookup_type, field_name):
        if lookup_type == 'week_day':
            # returns an integer from 1-7, where 1=Sunday.
            return "EXTRACT(ISODOW FROM %s)" % field_name
        elif lookup_type == 'week':
            # The ISO week of the year, an integer between 1 and 53.
            return "EXTRACT(ISOWEEK FROM %s)" % field_name
        elif lookup_type == 'quarter':
            return "EXTRACT(QUARTER FROM %s)" % field_name
        elif lookup_type == 'iso_year':
            return "EXTRACT(YEAR FROM %s)" % field_name
        else:
            return "EXTRACT(%s FROM %s)" % (lookup_type.upper(), field_name)

    def date_trunc_sql(self, lookup_type, field_name):
        if lookup_type in ('year', 'month'):
            return "TRUNC(%s, '%s')" % (field_name, lookup_type.upper())
        elif lookup_type == 'quarter':
            return "TRUNC(%s, 'Q')" % field_name
        elif lookup_type == 'week':
            return "TRUNC(%s, 'IW')" % field_name
        else:
            return "TRUNC(%s)" % field_name

    def datetime_cast_date_sql(self, field_name, tzname):
        return 'DATE(%s)' % field_name

    def _convert_field_to_tz(self, field_name, tzname):
        if settings.USE_TZ:
            field_name = "%s AT TIMEZONE '%s'" % (field_name, tzname)
        return field_name

    def datetime_cast_time_sql(self, field_name, tzname):
        field_name = self._convert_field_to_tz(field_name, tzname)
        return '(%s)::time' % field_name

    def date_interval_sql(self, timedelta):
        pass

    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        field_name = self._convert_field_to_tz(field_name, tzname)
        return self.date_extract_sql(lookup_type, field_name)

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        field_name = self._convert_field_to_tz(field_name, tzname)
        return self.date_trunc_sql(lookup_type, field_name)

    def time_trunc_sql(self, lookup_type, field_name):
        return "TRUNC(%s, '%s')::time" % (field_name, lookup_type.upper())

    def no_limit_value(self):
        return None

    def regex_lookup(self, lookup_type):
        if lookup_type == 'regex':
            match_option = "'c'"
        else:
            match_option = "'i'"
        return 'REGEXP_LIKE(%%s, %%s, %s)' % match_option

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        if tables:
            # Perform a single SQL 'TRUNCATE x, y, z...;' statement.  It allows
            # us to truncate tables referenced by a foreign key in any other
            # table.
            tables_sql = ', '.join(
                style.SQL_FIELD(self.quote_name(table)) for table in tables)
            if allow_cascade:
                sql = ['%s %s %s;' % (
                    style.SQL_KEYWORD('TRUNCATE'),
                    tables_sql,
                    style.SQL_KEYWORD('CASCADE'),
                )]
            else:
                sql = ['%s %s;' % (
                    style.SQL_KEYWORD('TRUNCATE'),
                    tables_sql,
                )]
            sql.extend(self.sequence_reset_by_name_sql(style, sequences))
            return sql
        else:
            return []
