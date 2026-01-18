from django.db.backends.base.base import *
from django_async_experiment.registry import from_codegen, generate_unasynced
from django.utils.asyncio import async_unsafe
from django_async_experiment import ASYNC_TRUTH_MARKER


class BaseDatabaseWrapperOverides:

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
