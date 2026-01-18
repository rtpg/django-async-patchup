#!/usr/bin/env python3
from django.db.backends.postgresql.base import *
from django_async_patchup.registry import from_codegen, generate_unasynced, just_patch


class DatabaseWrapperOverrides:

    @just_patch(onto=(DatabaseWrapper, "apool"))
    @property
    def apool(self):
        pool_options = self.settings_dict["OPTIONS"].get("pool")
        if self.alias == NO_DB_ALIAS or not pool_options:
            return None

        if self.alias not in self._aconnection_pools:
            if self.settings_dict.get("CONN_MAX_AGE", 0) != 0:
                raise ImproperlyConfigured(
                    "Pooling doesn't support persistent connections."
                )
            # Set the default options.
            if pool_options is True:
                pool_options = {}

            try:
                from psycopg_pool import AsyncConnectionPool
            except ImportError as err:
                raise ImproperlyConfigured(
                    "Error loading psycopg_pool module.\nDid you install psycopg[pool]?"
                ) from err

            connect_kwargs = self.get_connection_params(for_async=True)
            # Ensure we run in autocommit, Django properly sets it later on.
            connect_kwargs["autocommit"] = True
            enable_checks = self.settings_dict["CONN_HEALTH_CHECKS"]
            pool = AsyncConnectionPool(
                kwargs=connect_kwargs,
                open=False,  # Do not open the pool during startup.
                configure=self._aconfigure_connection,
                check=AsyncConnectionPool.check_connection if enable_checks else None,
                **pool_options,
            )
            # setdefault() ensures that multiple threads don't set this in
            # parallel. Since we do not open the pool during it's init above,
            # this means that at worst during startup multiple threads generate
            # pool objects and the first to set it wins.
            self._aconnection_pools.setdefault(self.alias, pool)

        return self._aconnection_pools[self.alias]

    @just_patch(onto=(DatabaseWrapper, "aclose_pool"))
    async def aclose_pool(self):
        if self.apool:
            await self.apool.close()
            del self._aconnection_pools[self.alias]

    @just_patch(onto=(DatabaseWrapper, "aget_database_version"))
    async def aget_database_version(self):
        """
        Return a tuple of the database's version.
        E.g. for pg_version 120004, return (12, 4).
        """
        pg_version = await self.apg_version
        return divmod(pg_version, 10000)

    @just_patch(onto=(DatabaseWrapper, "_get_sync_cursor_factory"))
    def _get_sync_cursor_factory(self, server_side_binding=None):
        if is_psycopg3 and server_side_binding is True:
            return ServerBindingCursor
        else:
            return Cursor

    @just_patch(onto=(DatabaseWrapper, "_get_async_cursor_factory"))
    def _get_async_cursor_factory(self, server_side_binding=None):
        if is_psycopg3 and server_side_binding is True:
            return AsyncServerBindingCursor
        else:
            return AsyncCursor

    @just_patch(onto=(DatabaseWrapper, "_get_cursor_factory"))
    def _get_cursor_factory(self, server_side_binding=None, for_async=False):
        if for_async and not is_psycopg3:
            raise ImproperlyConfigured(
                "Django requires psycopg >= 3 for ORM async support."
            )

        if for_async:
            return self._get_async_cursor_factory(server_side_binding)
        else:
            return self._get_sync_cursor_factory(server_side_binding)

    @just_patch(onto=(DatabaseWrapper, "aget_new_connection"))
    async def aget_new_connection(self, conn_params):
        isolation_level, set_isolation_level = self._get_isolation_level()
        self.isolation_level = isolation_level
        if self.apool:
            # If nothing else has opened the pool, open it now.
            await self.apool.open()
            connection = await self.apool.getconn()
        else:
            connection = await self.Database.AsyncConnection.connect(**conn_params)
        if set_isolation_level:
            connection.isolation_level = isolation_level
        return connection

    @just_patch(onto=(DatabaseWrapper, "aensure_timezone"))
    async def aensure_timezone(self):
        # Close the pool so new connections pick up the correct timezone.
        await self.aclose_pool()
        if self.connection is None:
            return False
        return await self._aconfigure_timezone(self.connection)

    @just_patch(onto=(DatabaseWrapper, "_aconfigure_timezone"))
    async def _aconfigure_timezone(self, connection):
        conn_timezone_name = connection.info.parameter_status("TimeZone")
        timezone_name = self.timezone_name
        if timezone_name and conn_timezone_name != timezone_name:
            async with connection.cursor() as cursor:
                await cursor.execute(self.ops.set_time_zone_sql(), [timezone_name])
            return True
        return False

    @just_patch(onto=(DatabaseWrapper, "_aclose"))
    async def _aclose(self):
    if self.aconnection is not None:
        # `wrap_database_errors` only works for `putconn` as long as there
        # is no `reset` function set in the pool because it is deferred
        # into a thread and not directly executed.
        with self.wrap_database_errors:
            if self.apool:
                # Ensure the correct pool is returned. This is a workaround
                # for tests so a pool can be changed on setting changes
                # (e.g. USE_TZ, TIME_ZONE).
                await self.aconnection._pool.putconn(self.aconnection)
                # Connection can no longer be used.
                self.aconnection = None
            else:
                return await self.aconnection.close()

    @just_patch(onto=(DatabaseWrapper, "ainit_connection_state"))
    async def ainit_connection_state(self):
        await super().ainit_connection_state()

        if self.aconnection is not None and not self.apool:
            commit = await self._aconfigure_connection(self.aconnection)

            if commit:
                autocommit = await self.aget_autocommit()
                if not autocommit:
                    await self.aconnection.commit()

    @just_patch(onto=(DatabaseWrapper, "create_async_cursor"))
    def create_async_cursor(self, name=None):
        if name:
            if self.settings_dict["OPTIONS"].get("server_side_binding") is not True:
                # psycopg >= 3 forces the usage of server-side bindings for
                # named cursors so a specialized class that implements
                # server-side cursors while performing client-side bindings
                # must be used if `server_side_binding` is disabled (default).
                cursor = AsyncServerSideCursor(
                    self.aconnection,
                    name=name,
                    scrollable=False,
                    withhold=self.aconnection.autocommit,
                )
            else:
                # In autocommit mode, the cursor will be used outside of a
                # transaction, hence use a holdable cursor.
                cursor = self.aconnection.cursor(
                    name, scrollable=False, withhold=self.aconnection.autocommit
                )
        else:
            cursor = self.aconnection.cursor()

        # Register the cursor timezone only if the connection disagrees, to
        # avoid copying the adapter map.
        tzloader = self.aconnection.adapters.get_loader(TIMESTAMPTZ_OID, Format.TEXT)
        if self.timezone != tzloader.timezone:
            register_tzloader(self.timezone, cursor)
        return cursor


    @just_patch(onto=(DatabaseWrapper, "achunked_cursor"))
    async def achunked_cursor(self):
        self._named_cursor_idx += 1
        # Get the current async task
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        # Current task can be none even if the current_task call didn't error
        if current_task:
            task_ident = str(id(current_task))
        else:
            task_ident = "sync"
        # Use that and the thread ident to get a unique name
        return self._acursor(
            name="_django_curs_%d_%s_%d"
            % (
                # Avoid reusing name in other threads / tasks
                threading.current_thread().ident,
                task_ident,
                self._named_cursor_idx,
            )
        )

    
    @just_patch(onto=(DatabaseWrapper, "_aset_autocommit"))
    async def _aset_autocommit(self, autocommit):
        with self.wrap_database_errors:
            await self.aconnection.set_autocommit(autocommit)

    @just_patch(onto=(DatabaseWrapper, "aclose_if_health_check_failed"))
    async def aclose_if_health_check_failed(self):
        if self.apool:
            # The pool only returns healthy connections.
            return
        return await super().aclose_if_health_check_failed()

    @just_patch(onto=(DatabaseWrapper, "apg_version"))
    @cached_property
    async def apg_version(self):
        async with self.atemporary_connection():
            return self.aconnection.info.server_version


class AsyncServerBindingCursor(CursorMixin, Database.AsyncClientCursor):
    pass

class AsyncCursor(CursorMixin, Database.AsyncClientCursor):
    pass

class AsyncServerSideCursor(
    CursorMixin,
    Database.client_cursor.ClientCursorMixin,
    Database.AsyncServerCursor,
):
    """
    psycopg >= 3 forces the usage of server-side bindings when using named
    cursors but the ORM doesn't yet support the systematic generation of
    prepareable SQL (#20516).

    ClientCursorMixin forces the usage of client-side bindings while
    AsyncServerCursor implements the logic required to declare and scroll
    through named cursors.

    Mixing ClientCursorMixin in wouldn't be necessary if Cursor allowed to
    specify how parameters should be bound instead, which AsyncServerCursor
    would inherit, but that's not the case.
    """

class AsyncCursorDebugWrapper(AsyncBaseCursorDebugWrapper):
    def copy(self, statement):
        with self.debug_sql(statement):
            return self.cursor.copy(statement)

