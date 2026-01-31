from asgiref.sync import sync_to_async
from asyncio import TaskGroup

from django.db.models.expressions import RawSQL

from django_async_backend.db import async_connections

from django_async_patchup import setup as django_async_setup
from time import time

from django.shortcuts import render, get_object_or_404
from django.db.models import Sum
from .models import Client


django_async_setup()


async def get_client_count(sleep_param):
    async with async_connections["default"].cursor() as cursor:
        await cursor.execute(f"SELECT pg_sleep({sleep_param})")
        await cursor.execute("SELECT COUNT(*) FROM biz_client")
        total_clients = (await cursor.fetchone())[0]
    return total_clients


def get_client_count_orm():
    return len(Client.objects.annotate(some_value=RawSQL("SELECT pg_sleep(5)", ())))


async def aget_client_count_orm(sleep_param):
    qs = Client.objects.annotate(
        some_value=RawSQL("SELECT pg_sleep(%s)", (sleep_param,))
    )
    return len([c async for c in qs])


def get_client_info():
    qs = Client.objects.annotate(invoice_total=Sum("invoices__total"))
    clients = [{"pk": c.pk, "name": c.name, "total": c.invoice_total or 0} for c in qs]
    return clients


async def overview(request):
    start_time = time()
    clients = await sync_to_async(get_client_info)()
    # test aget_client_count_orm
    await aget_client_count_orm(0.4)
    # get the client count twice just to demonstrate slowdown
    async with TaskGroup() as tg:
        # normally this takes 5 seconds...
        result_task = tg.create_task(get_client_count(5))
        # and here these take 2 seconds each
        # and _should_ be parallel, but...
        for _ in range(4):
            tg.create_task(aget_client_count_orm(2))
    # because the ORM is sync this takes 5 + some extra
    # due to the sync_to_async control barrier
    total_clients = result_task.result()
    duration = time() - start_time
    return render(
        request,
        "biz/overview.html",
        {"clients": clients, "total_clients": total_clients, "duration": duration},
    )


def client_detail(request, pk):
    client = get_object_or_404(Client, pk=pk)
    invoices = list(client.invoices.values("reference", "total"))
    data = {"pk": client.pk, "name": client.name, "invoices": invoices}
    return render(request, "biz/client.html", {"client": data})
