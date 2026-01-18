from django.db.models.sql.query import *
from django_async_patchup.registry import just_patch
from django_async_patchup.db import async_connections


class QueryOverrides:

    # TODO generate one from the other
    @just_patch(onto=(Query, "aget_compiler"))
    def aget_compiler(
        self, using=None, connection=None, elide_empty=True, raise_on_miss=True
    ):
        if using is None and connection is None:
            raise ValueError("Need either using or connection")
        if using:
            connection = async_connections.get_connection(using)
        return connection.ops.compiler(self.compiler)(
            self, connection, using, elide_empty
        )
