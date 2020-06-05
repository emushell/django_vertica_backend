from django.db.backends.base.schema import BaseDatabaseSchemaEditor


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_alter_column_type = "ALTER COLUMN %(column)s SET DATA TYPE %(type)s"
    sql_unique_constraint = "UNIQUE (%(columns)s) ENABLED"
    sql_create_unique = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s UNIQUE (%(columns)s) ENABLED"
    sql_create_pk = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s PRIMARY KEY (%(columns)s) ENABLED"

    custom_sql_column_null = " NULL"
    custom_sql_column_not_null = " NOT NULL"
    custom_sql_column_pk = " PRIMARY KEY ENABLED"
    custom_sql_column_unique = " UNIQUE ENABLED"

    def alter_field(self, model, old_field, new_field, strict=False):
        """
        Vertica do not allow alter column type if it is used in constraints such as UNIQUE.
        In order to all work, the constraint is dropped, column altered, constraint recreated.
        """
        curr = self.connection.cursor()
        result = curr.execute("""
        select cc.constraint_name
          from constraint_columns cc
         where 1=1
           and cc.table_name = '%s'
           and cc.constraint_type = 'u'
           and cc.column_name = '%s'""" % (model._meta.db_table, new_field.column)).fetchone()
        if result:
            constraint_name = result[0]
            drop_statement = self._delete_constraint_sql(self.sql_delete_unique, model, constraint_name)
            self.execute(drop_statement)
            super().alter_field(model, old_field, new_field, strict)
            create_statement = self._create_unique_sql(model, [new_field.column], constraint_name)
            self.execute(create_statement)
        else:
            super().alter_field(model, old_field, new_field, strict)

    def column_sql(self, model, field, include_default=False):
        """
        Take a field and return its column definition.
        The field must already have had set_attributes_from_name() called.
        """
        # Get the column's type and use that as the basis of the SQL
        db_params = field.db_parameters(connection=self.connection)
        sql = db_params['type']
        params = []
        # Check for fields that aren't actually columns (e.g. M2M)
        if sql is None:
            return None, None
        # Work out nullability
        null = field.null
        # If we were told to include a default value, do so
        include_default = include_default and not self.skip_default(field)
        if include_default:
            default_value = self.effective_default(field)
            if default_value is not None:
                if self.connection.features.requires_literal_defaults:
                    # Some databases can't take defaults as a parameter (oracle)
                    # If this is the case, the individual schema backend should
                    # implement prepare_default
                    sql += " DEFAULT %s" % self.prepare_default(default_value)
                else:
                    sql += " DEFAULT %s"
                    params += [default_value]
        # Oracle treats the empty string ('') as null, so coerce the null
        # option whenever '' is a possible value.
        if (field.empty_strings_allowed and not field.primary_key and
                self.connection.features.interprets_empty_strings_as_nulls):
            null = True
        if null and not self.connection.features.implied_column_null:
            sql += self.custom_sql_column_null
        elif not null:
            sql += self.custom_sql_column_not_null
        # Primary key/unique outputs
        if field.primary_key:
            sql += self.custom_sql_column_pk
        elif field.unique:
            sql += self.custom_sql_column_unique
        # Return the sql
        return sql, params

    def prepare_default(self, value):
        pass

    def quote_value(self, value):
        pass

    def add_index(self, model, index):
        pass

    def remove_index(self, model, index):
        pass

    def _model_indexes_sql(self, model):
        """
         Vertica dose not support INDEX's.
         Skip all statements which are related to index creation or manipulation.
        """
        return []