#!/usr/bin/env python3
# XXX move to right spot
from django.db.backends.postgresql.operations import *
from .postgresql.base import *
from .postgresql import *
from django_async_patchup.registry import from_codegen, generate_unasynced


class DatabaseOperationsOverrides:

    @from_codegen(original=DatabaseOperations.fetch_returned_insert_rows)
    def fetch_returned_insert_rows(self, cursor):
        """
        Given a cursor object that has just performed an INSERT...RETURNING
        statement into a table, return the tuple of returned data.
        """
        return cursor.fetchall()

    @generate_unasynced(sync_variant=DatabaseOperations.fetch_returned_insert_rows)
    async def afetch_returned_insert_rows(self, cursor):
        """
        Given a cursor object that has just performed an INSERT...RETURNING
        statement into a table, return the tuple of returned data.
        """
        return await cursor.fetchall()
