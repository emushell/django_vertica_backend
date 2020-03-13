from django.db.backends.base.features import BaseDatabaseFeatures


class DatabaseFeatures(BaseDatabaseFeatures):
    has_bulk_insert = False
    # has_select_for_update = True
