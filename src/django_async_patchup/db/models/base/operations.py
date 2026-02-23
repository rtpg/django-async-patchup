# XXX fix path of this file
from django_async_patchup.registry import generate_unasynced
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.postgresql.operations import DatabaseOperations as PgDatabaseOperations


class BaseDatabaseOperationsOverrides:

    @generate_unasynced(
        sync_variant=BaseDatabaseOperations.fetch_returned_insert_columns
    )
    async def afetch_returned_insert_columns(self, cursor, returning_params):
        """
        Given a cursor object that has just performed an INSERT...RETURNING
        statement into a table, return the newly created data.
        """
        return await cursor.fetchone()

    @generate_unasynced(
        sync_variant=PgDatabaseOperations.fetch_returned_insert_rows
    )
    async def afetch_returned_insert_rows(self, cursor):
        """
        Given a cursor object that has just performed an INSERT...RETURNING
        statement into a table, return the tuple of returned data.
        """
        return await cursor.fetchall()
