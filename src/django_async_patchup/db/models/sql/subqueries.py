from django.db.models.sql.subqueries import *

from django_async_patchup.registry import from_codegen, generate_unasynced


class DeleteQueryOverrides:

    @generate_unasynced(sync_variant=DeleteQuery.do_query)
    async def ado_query(self, table, where, using):
        self.alias_map = {table: self.alias_map[table]}
        self.where = where

        return await self.aget_compiler(using).aexecute_sql(ROW_COUNT)

    @generate_unasynced(sync_variant=DeleteQuery.delete_batch)
    async def adelete_batch(self, pk_list, using):
        """
        Set up and execute delete queries for all the objects in pk_list.

        More than one physical query may be executed if there are a
        lot of values in pk_list.
        """
        # number of objects deleted
        num_deleted = 0
        field = self.get_meta().pk
        for offset in range(0, len(pk_list), GET_ITERATOR_CHUNK_SIZE):
            self.clear_where()
            self.add_filter(
                f"{field.attname}__in",
                pk_list[offset : offset + GET_ITERATOR_CHUNK_SIZE],
            )
            num_deleted += await self.ado_query(
                self.get_meta().db_table, self.where, using=using
            )
        return num_deleted
