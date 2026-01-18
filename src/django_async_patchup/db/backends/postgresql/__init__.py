#!/usr/bin/env python3
from django_async_patchup.registry import generate_unasynced
from django.db.backends.postgresql.operations import *


@generate_unasynced(sync_variant=DatabaseOperations.fetch_returned_insert_rows)
async def afetch_returned_insert_rows(self, cursor):
    """
    Given a cursor object that has just performed an INSERT...RETURNING
    statement into a table, return the tuple of returned data.
    """
    return await cursor.fetchall()
