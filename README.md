Django Async Patchup
====================

This currently works with [this specific branch](https://github.com/django/django/compare/main...rtpg:django:async-experiment) (that currently includes a bunch of things that I'm trying to winnow down), the idea is that we want this to work for exactly Django without patches. Should be totally doable!


Usage
-----

- Be on the right version of Django
- In your `asgi.py` (for example), set up the monkey patch

``` python
from django_async_patchup import setup as setup_async
setup_async()
```


You can now do something like the following:

```python

async def get_invoice_summary(client: Client):
    from django_async_patchup.db import new_connection, allow_async_db_commits

    with allow_async_db_commits():
        async with new_connection():
            my_invs = [
                inv
                async for inv in Invoice.objects.filter(client=client).annotate(
                    other=RawSQL("pg_sleep(.1)", [])
                )
            ]
            return {"count": len(my_invs)}

```

More details will come as things coalesce
