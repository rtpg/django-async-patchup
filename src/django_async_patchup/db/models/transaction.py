class MarkForRollbackOnError:
    def __init__(self, using):
        self.using = using

    def __enter__(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            connection = await aget_connection(self.using)
            if connection.in_atomic_block:
                connection.needs_rollback = True
                connection.rollback_exc = exc_val

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            connection = get_connection(self.using)
            if connection.in_atomic_block:
                connection.needs_rollback = True
                connection.rollback_exc = exc_val


def amark_for_rollback_on_error(using=None):
    return MarkForRollbackOnError(using=using)
