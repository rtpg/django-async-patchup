import pytest
import pytest_asyncio
from asgiref.sync import sync_to_async
from django.db import connections
from django_async_backend.db import async_connections


@pytest_asyncio.fixture(autouse=True)
async def close_all_db_connections(django_db_setup):
    """
    HACK we should figure out how to not have to do this.

    Close database connections that pytest-django misses for async tests:

    1. async_connections["default"] - opened by aget_compiler / aexecute_sql via
       django_async_backend. These are separate from django.db.connections and
       pytest-django never touches them.

    2. Thread-pool thread connections - sync_to_async() runs in a thread pool whose
       threads stay alive between calls, keeping their thread-local
       django.db.connections["default"] open. pytest-django only calls
       close_old_connections() in the main thread, not in the thread pool threads.
       Running close_all() via sync_to_async() closes the connection in the
       thread-sensitive thread that processes those calls.
    """
    yield
    await async_connections.close_all()
    await sync_to_async(connections.close_all)()


@pytest.fixture(scope="session", autouse=True)
def close_main_thread_connections(django_db_setup):
    """
    HACK we should figure out how to not have to do this.

    Close the main-thread sync connection before teardown_databases() drops the DB.

    pytest-django's _post_teardown() reopens a connection to roll back the test
    transaction but never closes it (because _should_reload_connections() returns
    False for PostgreSQL). This session-scoped fixture depends on django_db_setup,
    so its teardown runs just before teardown_databases() does, giving us a window
    to close whatever _post_teardown() left open.
    """
    yield
    connections.close_all()
