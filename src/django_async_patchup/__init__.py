# all changes should end up in here!


from django_async_patchup.registry import async_methods


def setup():
    # XXX do all the imports
    from django_async_patchup.db.models.query import QuerySetOverrides
    from django_async_patchup.db.models.deletion import CollectorOverrides
    from django_async_patchup.db.backends.base.base import (
        BaseDatabaseWrapperOverides,
    )

    from django_async_patchup.db.models.sql.subqueries import DeleteQueryOverrides
    from django_async_patchup.db.models.base.operations import (
        BaseDatabaseOperationsOverrides,
    )

    for meth in async_methods():
        meth.apply_patch()
    print("TODO: monkeypatch")


def hello():
    print("Hi")


# this marker gets replaced by False when unasyncifying a function
ASYNC_TRUTH_MARKER = True
