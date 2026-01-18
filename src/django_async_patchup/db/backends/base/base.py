from django.db.backends.base.base import *
from django_async_patchup.registry import from_codegen, generate_unasynced, just_patch
from django_async_patchup.db.backends.utils import (
    AsyncCursorCtx,
    AsyncCursorWrapper,
    AsyncCursorDebugWrapper,
)
from django.utils.asyncio import async_unsafe
from django_async_patchup import ASYNC_TRUTH_MARKER


class BaseDatabaseWrapperOverides:

    @just_patch(onto=(BaseDatabaseWrapper, "_validate_if_database_version_supported"))
    def _validate_database_version_supported(self, db_version):
        if (
            self.features.minimum_database_version is not None
            and db_version < self.features.minimum_database_version
        ):
            str_db_version = ".".join(map(str, db_version))
            min_db_version = ".".join(map(str, self.features.minimum_database_version))
            raise NotSupportedError(
                f"{self.display_name} {min_db_version} or later is required "
                f"(found {str_db_version})."
            )

    @just_patch(onto=BaseDatabaseWrapper)
    def _pre_connect(self):
        self.check_settings()
        # In case the previous connection was closed while in an atomic block
        self.in_atomic_block = False
        self.savepoint_ids = []
        self.atomic_blocks = []
        self.needs_rollback = False
        # Reset parameters defining when to close/health-check the connection.
        self.health_check_enabled = self.settings_dict["CONN_HEALTH_CHECKS"]
        max_age = self.settings_dict["CONN_MAX_AGE"]
        self.close_at = None if max_age is None else time.monotonic() + max_age
        self.closed_in_transaction = False
        self.errors_occurred = False
        # New connections are healthy.
        self.health_check_done = True

    @just_patch(onto=BaseDatabaseWrapper)
    @async_unsafe
    def connect(self):
        """Connect to the database. Assume that the connection is closed."""
        # Check for invalid configurations.
        self._pre_connect()
        # Establish the connection
        conn_params = self.get_connection_params()
        self.connection = self.get_new_connection(conn_params)
        self.set_autocommit(self.settings_dict["AUTOCOMMIT"])
        self.init_connection_state()
        connection_created.send(sender=self.__class__, connection=self)

        self.run_on_commit = []

    @from_codegen(original=BaseDatabaseWrapper.ensure_connection)
    @async_unsafe
    def ensure_connection(self):
        """Guarantee that a connection to the database is established."""
        if self.connection is None:
            if self.in_atomic_block and self.closed_in_transaction:
                raise ProgrammingError(
                    "Cannot open a new connection in an atomic block."
                )
            with self.wrap_database_errors:
                self.connect()

    @generate_unasynced(
        async_unsafe=True, sync_variant=BaseDatabaseWrapper.ensure_connection
    )
    async def aensure_connection(self):
        """Guarantee that a connection to the database is established."""
        if self.aconnection is None:
            if self.in_atomic_block and self.closed_in_transaction:
                raise ProgrammingError(
                    "Cannot open a new connection in an atomic block."
                )
            with self.wrap_database_errors:
                await self.aconnect()

    @from_codegen(original=BaseDatabaseWrapper.connect)
    @async_unsafe
    def connect(self):
        """Connect to the database. Assume that the connection is closed."""
        # Check for invalid configurations.
        self._pre_connect()
        # Establish the connection
        conn_params = self.get_connection_params()
        self.connection = self.get_new_connection(conn_params)
        self.set_autocommit(self.settings_dict["AUTOCOMMIT"])
        self.init_connection_state()
        connection_created.send(sender=self.__class__, connection=self)

        self.run_on_commit = []

    @generate_unasynced(async_unsafe=True, sync_variant=BaseDatabaseWrapper.connect)
    async def aconnect(self):
        """Connect to the database. Assume that the connection is closed."""
        # Check for invalid configurations.
        self._pre_connect()
        if ASYNC_TRUTH_MARKER:
            # Establish the connection
            conn_params = self.get_connection_params(for_async=True)
        else:
            # Establish the connection
            conn_params = self.get_connection_params()
        self.aconnection = await self.aget_new_connection(conn_params)
        await self.aset_autocommit(self.settings_dict["AUTOCOMMIT"])
        await self.ainit_connection_state()
        await connection_created.asend(sender=self.__class__, connection=self)

        self.run_on_commit = []

    @from_codegen(original=BaseDatabaseWrapper._commit)
    def _commit(self):
        if self.connection is not None:
            with debug_transaction(self, "COMMIT"), self.wrap_database_errors:
                return self.connection.commit()

    @generate_unasynced(sync_variant=BaseDatabaseWrapper._commit)
    async def _acommit(self):
        if self.aconnection is not None:
            with debug_transaction(self, "COMMIT"), self.wrap_database_errors:
                return await self.aconnection.commit()

    @from_codegen(original=BaseDatabaseWrapper.init_connection_state)
    def init_connection_state(self):
        """Initialize the database connection settings."""
        global RAN_DB_VERSION_CHECK
        if self.alias not in RAN_DB_VERSION_CHECK:
            self.check_database_version_supported()
            RAN_DB_VERSION_CHECK.add(self.alias)

    @generate_unasynced(sync_variant=BaseDatabaseWrapper.init_connection_state)
    async def ainit_connection_state(self):
        """Initialize the database connection settings."""
        global RAN_DB_VERSION_CHECK
        if self.alias not in RAN_DB_VERSION_CHECK:
            await self.acheck_database_version_supported()
            RAN_DB_VERSION_CHECK.add(self.alias)

    @from_codegen(original=BaseDatabaseWrapper._rollback)
    def _rollback(self):
        if self.connection is not None:
            with debug_transaction(self, "ROLLBACK"), self.wrap_database_errors:
                return self.connection.rollback()

    @generate_unasynced(sync_variant=BaseDatabaseWrapper._rollback)
    async def _arollback(self):
        if self.aconnection is not None:
            with debug_transaction(self, "ROLLBACK"), self.wrap_database_errors:
                return await self.aconnection.rollback()

    @from_codegen(original=BaseDatabaseWrapper._close)
    def _close(self):
        print(f"YYY {id(self)} BDW CLOSE")
        if self.connection is not None:
            with self.wrap_database_errors:
                return self.connection.close()

    @generate_unasynced(sync_variant=BaseDatabaseWrapper._close)
    async def _aclose(self):
        print(f"YYY {id(self)} BDW CLOSE")
        if self.aconnection is not None:
            with self.wrap_database_errors:
                return await self.aconnection.close()

    @from_codegen(original=BaseDatabaseWrapper.commit)
    @async_unsafe
    def commit(self):
        """Commit a transaction and reset the dirty flag."""
        self.validate_thread_sharing()
        self.validate_no_atomic_block()
        self._commit()
        # A successful commit means that the database connection works.
        self.errors_occurred = False
        self.run_commit_hooks_on_set_autocommit_on = True

    @generate_unasynced(sync_variant=BaseDatabaseWrapper.commit, async_unsafe=True)
    async def acommit(self):
        """Commit a transaction and reset the dirty flag."""
        self.validate_thread_sharing()
        self.validate_no_atomic_block()
        await self._acommit()
        # A successful commit means that the database connection works.
        self.errors_occurred = False
        self.run_commit_hooks_on_set_autocommit_on = True

    @from_codegen(original=BaseDatabaseWrapper.rollback)
    @async_unsafe
    def rollback(self):
        """Roll back a transaction and reset the dirty flag."""
        self.validate_thread_sharing()
        self.validate_no_atomic_block()
        self._rollback()
        # A successful rollback means that the database connection works.
        self.errors_occurred = False
        self.needs_rollback = False
        self.run_on_commit = []

    @generate_unasynced(sync_variant=BaseDatabaseWrapper.rollback, async_unsafe=True)
    async def arollback(self):
        """Roll back a transaction and reset the dirty flag."""
        self.validate_thread_sharing()
        self.validate_no_atomic_block()
        await self._arollback()
        # A successful rollback means that the database connection works.
        self.errors_occurred = False
        self.needs_rollback = False
        self.run_on_commit = []

    @from_codegen(original=BaseDatabaseWrapper.close)
    @async_unsafe
    def close(self):
        """Close the connection to the database."""
        self.validate_thread_sharing()
        self.run_on_commit = []

        # Don't call validate_no_atomic_block() to avoid making it difficult
        # to get rid of a connection in an invalid state. The next connect()
        # will reset the transaction state anyway.
        if self.closed_in_transaction or self.connection is None:
            return
        try:
            self._close()
        finally:
            if self.in_atomic_block:
                self.closed_in_transaction = True
                self.needs_rollback = True
            else:
                self.connection = None

    @generate_unasynced(sync_variant=BaseDatabaseWrapper.close, async_unsafe=True)
    async def aclose(self):
        """Close the connection to the database."""
        self.validate_thread_sharing()
        self.run_on_commit = []

        # Don't call validate_no_atomic_block() to avoid making it difficult
        # to get rid of a connection in an invalid state. The next connect()
        # will reset the transaction state anyway.
        if self.closed_in_transaction or self.aconnection is None:
            return
        try:
            await self._aclose()
        finally:
            if self.in_atomic_block:
                self.closed_in_transaction = True
                self.needs_rollback = True
            else:
                self.aconnection = None

    BaseDatabaseWrapper.supports_async = False
    BaseDatabaseWrapper._aconnection_pools = {}
    BaseDatabaseWrapper.aconnection = None

    @just_patch(onto=(BaseDatabaseWrapper, "aget_database_version"))
    async def aget_database_version(self):
        """Return a tuple of the database's version."""
        raise NotSupportedError(
            "subclasses of BaseDatabaseWrapper may require an aget_database_version() "
            "method."
        )

    @just_patch(onto=(BaseDatabaseWrapper, "acheck_database_version_supported"))
    async def acheck_database_version_supported(self):
        """
        Raise an error if the database version isn't supported by this
        version of Django.
        """
        db_version = await self.aget_database_version()
        self._validate_database_version_supported(db_version)

    @just_patch(onto=(BaseDatabaseWrapper, "aget_new_connection"))
    async def aget_new_connection(self, conn_params):
        """Open a connection to the database."""
        raise NotSupportedError(
            "subclasses of BaseDatabaseWrapper may require an aget_new_connection() "
            "method"
        )

    @just_patch(onto=(BaseDatabaseWrapper, "create_async_cursor"))
    def create_async_cursor(self, name=None):
        """Create a cursor. Assume that a connection is established."""
        raise NotSupportedError(
            "subclasses of BaseDatabaseWrapper may require a "
            "create_async_cursor() method"
        )

    # TODO unasyncify
    @just_patch(onto=(BaseDatabaseWrapper, "_aprepare_cursor"))
    def _aprepare_cursor(self, cursor) -> utils.AsyncCursorWrapper:
        """
        Validate the connection is usable and perform database cursor wrapping.
        """

        self.validate_thread_sharing()
        if self.queries_logged:
            wrapped_cursor = self.make_debug_async_cursor(cursor)
        else:
            wrapped_cursor = self.make_async_cursor(cursor)
        return wrapped_cursor

    # TODO unasyncify
    @just_patch(onto=(BaseDatabaseWrapper, "_acursor"))
    def _acursor(self, name=None) -> utils.AsyncCursorCtx:
        return AsyncCursorCtx(self, name)

    # TODO unasyncify
    @just_patch(onto=(BaseDatabaseWrapper, "acursor"))
    def acursor(self) -> utils.AsyncCursorCtx:
        """Create an async cursor, opening a connection if necessary."""
        # if ASYNC_TRUTH_MARKER:
        #     self.validate_no_atomic_block()
        return self._acursor()

    # TODO unasyncify
    @just_patch(onto=(BaseDatabaseWrapper, "_asavepoint"))
    async def _asavepoint(self, sid):
        async with self.acursor() as cursor:
            await cursor.aexecute(self.ops.savepoint_create_sql(sid))

    @just_patch(onto=(BaseDatabaseWrapper, "_asavepoint_rollback"))
    async def _asavepoint_rollback(self, sid):
        async with self.acursor() as cursor:
            await cursor.aexecute(self.ops.savepoint_rollback_sql(sid))

    @just_patch(onto=(BaseDatabaseWrapper, "_asavepoint_commit"))
    async def _asavepoint_commit(self, sid):
        async with self.acursor() as cursor:
            await cursor.aexecute(self.ops.savepoint_commit_sql(sid))

    @just_patch(onto=(BaseDatabaseWrapper, "_asavepoint_allowed"))
    async def _asavepoint_allowed(self):
        # Savepoints cannot be created outside a transaction
        return self.features.uses_savepoints and not (await self.aget_autocommit())

    @just_patch(onto=(BaseDatabaseWrapper, "asavepoint"))
    async def asavepoint(self):
        """
        Create a savepoint inside the current transaction. Return an
        identifier for the savepoint that will be used for the subsequent
        rollback or commit. Do nothing if savepoints are not supported.
        """
        if not (await self._asavepoint_allowed()):
            return

        thread_ident = _thread.get_ident()
        tid = str(thread_ident).replace("-", "")

        self.savepoint_state += 1
        sid = "s%s_x%d" % (tid, self.savepoint_state)

        self.validate_thread_sharing()
        await self._asavepoint(sid)

        return sid

    @just_patch(onto=(BaseDatabaseWrapper, "asavepoint_rollback"))
    async def asavepoint_rollback(self, sid):
        """
        Roll back to a savepoint. Do nothing if savepoints are not supported.
        """
        if not (await self._asavepoint_allowed()):
            return

        self.validate_thread_sharing()
        await self._asavepoint_rollback(sid)

        # Remove any callbacks registered while this savepoint was active.
        self.run_on_commit = [
            (sids, func, robust)
            for (sids, func, robust) in self.run_on_commit
            if sid not in sids
        ]

    @just_patch(onto=(BaseDatabaseWrapper, "asavepoint_commit"))
    async def asavepoint_commit(self, sid):
        """
        Release a savepoint. Do nothing if savepoints are not supported.
        """
        if not (await self._asavepoint_allowed()):
            return

        self.validate_thread_sharing()
        await self._asavepoint_commit(sid)

    @just_patch(onto=(BaseDatabaseWrapper, "_aset_autocommit"))
    async def _aset_autocommit(self, autocommit):
        """
        Backend-specific implementation to enable or disable autocommit.
        """
        raise NotSupportedError(
            "subclasses of BaseDatabaseWrapper may require an _aset_autocommit() method"
        )

    @just_patch(onto=(BaseDatabaseWrapper, "aget_autocommit"))
    async def aget_autocommit(self):
        """Get the autocommit state."""
        await self.aensure_connection()
        # print(f"aget_autocommit() <- {self.autocommit}")
        return self.autocommit

    @just_patch(onto=(BaseDatabaseWrapper, "aset_autocommit"))
    async def aset_autocommit(
        self, autocommit, force_begin_transaction_with_broken_autocommit=False
    ):
        """
        Enable or disable autocommit.

        The usual way to start a transaction is to turn autocommit off.
        SQLite does not properly start a transaction when disabling
        autocommit. To avoid this buggy behavior and to actually enter a new
        transaction, an explicit BEGIN is required. Using
        force_begin_transaction_with_broken_autocommit=True will issue an
        explicit BEGIN with SQLite. This option will be ignored for other
        backends.
        """
        # print(f"{id(self)}.aset_autocommit({autocommit})")
        # if autocommit is False:
        #     raise ValueError("FALSE")
        self.validate_no_atomic_block()
        await self.aclose_if_health_check_failed()
        await self.aensure_connection()

        start_transaction_under_autocommit = (
            force_begin_transaction_with_broken_autocommit
            and not autocommit
            and hasattr(self, "_astart_transaction_under_autocommit")
        )

        if start_transaction_under_autocommit:
            await self._astart_transaction_under_autocommit()
        elif autocommit:
            await self._aset_autocommit(autocommit)
        else:
            with debug_transaction(self, "BEGIN"):
                await self._aset_autocommit(autocommit)
        self.autocommit = autocommit

        if autocommit and self.run_commit_hooks_on_set_autocommit_on:
            self.run_and_clear_commit_hooks()
            self.run_commit_hooks_on_set_autocommit_on = False

    @just_patch(onto=(BaseDatabaseWrapper, "aclose_if_health_check_failed"))
    async def aclose_if_health_check_failed(self):
        """Close existing connection if it fails a health check."""
        if (
            self.aconnection is None
            or not self.health_check_enabled
            or self.health_check_done
        ):
            return

        is_usable = await self.ais_usable()
        if not is_usable:
            await self.aclose()
        self.health_check_done = True

    @just_patch(onto=(BaseDatabaseWrapper, "make_debug_async_cursor"))
    def make_debug_async_cursor(self, cursor):
        """Create a cursor that logs all queries in self.queries_log."""
        return AsyncCursorDebugWrapper(cursor, self)

    @just_patch(onto=(BaseDatabaseWrapper, "make_async_cursor"))
    def make_async_cursor(self, cursor):
        """Create a cursor without debug logging."""
        return AsyncCursorWrapper(cursor, self)

    @just_patch(onto=(BaseDatabaseWrapper, "atemporary_connection"))
    @asynccontextmanager
    async def atemporary_connection(self):
        """
        Context manager that ensures that a connection is established, and
        if it opened one, closes it to avoid leaving a dangling connection.
        This is useful for operations outside of the request-response cycle.

        Provide a cursor: async with self.atemporary_connection() as cursor: ...
        """
        # unused

        must_close = self.aconnection is None
        try:
            async with self.acursor() as cursor:
                yield cursor
        finally:
            if must_close:
                await self.aclose()
