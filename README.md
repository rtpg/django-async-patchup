Django Async Patchup
====================

This project patches Django's ORM to provide "native" async methods. It relies on [django-async-backend](github.com/Arfey/django-async-backend) to provide the async connection, and relies on [django-unasyncify](https://django-unasyncify.readthedocs.io/en/latest/) to ensure that the async variants to the ORM are "in sync" with the sync versions

This currently targets Django 5.2.10. I have more coverage I will look into expanding what Django versions are supported.


Usage
-----

- Be on the right version of Django
- Set up Postges's async backend/django-async-backend
- In your `asgi.py` (for example), set up the monkey patch

``` python
from django_async_patchup import setup as setup_async
setup_async()
```


You can now do something like the following:

```python

async def get_invoice_summary(client: Client):
        my_invs = [
            inv
            async for inv in Invoice.objects.filter(client=client).annotate(
                other=RawSQL("pg_sleep(1)", [])
            )
        ]
        return {"count": len(my_invs)}

```

More details will come as things coalesce
