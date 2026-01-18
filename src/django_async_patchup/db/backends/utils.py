#!/usr/bin/env python3
from django.db.backends.utils import *


class AsyncCursorCtx:
    """
    Asynchronous context manager to hold an async cursor.
    """

    def __init__(self, db, name=None):
        self.db = db
        self.name = name
        self.wrap_database_errors = self.db.wrap_database_errors

    async def __aenter__(self) -> "AsyncCursorWrapper":
        await self.db.aclose_if_health_check_failed()
        await self.db.aensure_connection()
        self.wrap_database_errors.__enter__()
        return self.db._aprepare_cursor(self.db.create_async_cursor(self.name))

    async def __aexit__(self, type, value, traceback):
        self.wrap_database_errors.__exit__(type, value, traceback)


class AsyncCursorWrapper(CursorWrapper):
    async def _aexecute(self, sql, params, *ignored_wrapper_args):
        # Raise a warning during app initialization (stored_app_configs is only
        # ever set during testing).
        if not apps.ready and not apps.stored_app_configs:
            warnings.warn(self.APPS_NOT_READY_WARNING_MSG, category=RuntimeWarning)
        self.db.validate_no_broken_transaction()
        with self.db.wrap_database_errors:
            if params is None:
                # params default might be backend specific.
                return await self.cursor.execute(sql)
            else:
                return await self.cursor.execute(sql, params)

    async def _aexecute_with_wrappers(self, sql, params, many, executor):
        context = {"connection": self.db, "cursor": self}
        for wrapper in reversed(self.db.execute_wrappers):
            executor = functools.partial(wrapper, executor)
        return await executor(sql, params, many, context)

    async def aexecute(self, sql, params=None):
        return await self._aexecute_with_wrappers(
            sql, params, many=False, executor=self._aexecute
        )

    async def _aexecutemany(self, sql, param_list, *ignored_wrapper_args):
        # Raise a warning during app initialization (stored_app_configs is only
        # ever set during testing).
        if not apps.ready and not apps.stored_app_configs:
            warnings.warn(self.APPS_NOT_READY_WARNING_MSG, category=RuntimeWarning)
        self.db.validate_no_broken_transaction()
        with self.db.wrap_database_errors:
            return await self.cursor.executemany(sql, param_list)

    async def aexecutemany(self, sql, param_list):
        return await self._aexecute_with_wrappers(
            sql, param_list, many=True, executor=self._aexecutemany
        )

    async def afetchone(self, *args, **kwargs):
        return await self.cursor.fetchone(*args, **kwargs)

    async def afetchmany(self, *args, **kwargs):
        return await self.cursor.fetchmany(*args, **kwargs)

    async def afetchall(self, *args, **kwargs):
        return await self.cursor.fetchall(*args, **kwargs)

    async def acopy(self, *args, **kwargs):
        return await self.cursor.copy(*args, **kwargs)

    async def astream(self, *args, **kwargs):
        return await self.cursor.stream(*args, **kwargs)

    async def ascroll(self, *args, **kwargs):
        return await self.cursor.ascroll(*args, **kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, type, value, traceback):
        try:
            await self.aclose()
        except self.db.Database.Error:
            pass

    async def aclose(self):
        with unblock_sync_ops():
            await self.close()


class AsyncCursorDebugWrapper(AsyncCursorWrapper):
    # XXX callproc isn't instrumented at this time.

    async def aexecute(self, sql, params=None):
        with self.debug_sql(sql, params, use_last_executed_query=True):
            return await super().aexecute(sql, params)

    async def aexecutemany(self, sql, param_list):
        with self.debug_sql(sql, param_list, many=True):
            return await super().aexecutemany(sql, param_list)

    @contextmanager
    def debug_sql(
        self, sql=None, params=None, use_last_executed_query=False, many=False
    ):
        start = time.monotonic()
        try:
            yield
        finally:
            stop = time.monotonic()
            duration = stop - start
            if use_last_executed_query:
                sql = self.db.ops.last_executed_query(self.cursor, sql, params)
            try:
                times = len(params) if many else ""
            except TypeError:
                # params could be an iterator.
                times = "?"
            self.db.queries_log.append(
                {
                    "sql": "%s times: %s" % (times, sql) if many else sql,
                    "time": "%.3f" % duration,
                    "async": True,
                }
            )
            logger.debug(
                "(%.3f) %s; args=%s; alias=%s; async=True",
                duration,
                sql,
                params,
                self.db.alias,
                extra={
                    "duration": duration,
                    "sql": sql,
                    "params": params,
                    "alias": self.db.alias,
                    "async": True,
                },
            )
