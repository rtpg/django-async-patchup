#!/usr/bin/env python3
"""Tests for @generate_unasynced queryset methods."""
import pytest
from decimal import Decimal
from biz.models import Client, Invoice

import django_async_patchup

django_async_patchup.setup()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acreate():
    client = await Client.objects.acreate(name="Created Client")
    assert client.pk is not None
    assert client.name == "Created Client"
    assert await Client.objects.filter(pk=client.pk).aexists()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aget():
    client = await Client.objects.acreate(name="Get Client")
    fetched = await Client.objects.aget(pk=client.pk)
    assert fetched.pk == client.pk
    assert fetched.name == "Get Client"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aget_raises_on_missing():
    with pytest.raises(Client.DoesNotExist):
        await Client.objects.aget(name="Does Not Exist XYZ")


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aget_raises_on_multiple():
    await Client.objects.acreate(name="Dupe")
    await Client.objects.acreate(name="Dupe")
    with pytest.raises(Client.MultipleObjectsReturned):
        await Client.objects.aget(name="Dupe")


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_adelete():
    client = await Client.objects.acreate(name="Delete Me")
    pk = client.pk
    deleted_count, _ = await Client.objects.filter(pk=pk).adelete()
    assert deleted_count == 1
    assert not await Client.objects.filter(pk=pk).aexists()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate():
    client = await Client.objects.acreate(name="Before Update")
    updated = await Client.objects.filter(pk=client.pk).aupdate(name="After Update")
    assert updated == 1
    refreshed = await Client.objects.aget(pk=client.pk)
    assert refreshed.name == "After Update"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_create():
    clients = await Client.objects.abulk_create([
        Client(name="Bulk A"),
        Client(name="Bulk B"),
        Client(name="Bulk C"),
    ])
    assert len(clients) == 3
    assert await Client.objects.filter(name__startswith="Bulk ").acount() == 3


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_update():
    clients = await Client.objects.abulk_create([
        Client(name="BU Before A"),
        Client(name="BU Before B"),
    ])
    for c in clients:
        c.name = c.name.replace("Before", "After")
    updated = await Client.objects.abulk_update(clients, fields=["name"])
    assert updated == 2
    names = {c.name async for c in Client.objects.filter(name__startswith="BU ")}
    assert names == {"BU After A", "BU After B"}


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aget_or_create_creates():
    client, created = await Client.objects.aget_or_create(name="GOC New")
    assert created is True
    assert client.pk is not None


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aget_or_create_gets():
    existing = await Client.objects.acreate(name="GOC Existing")
    client, created = await Client.objects.aget_or_create(name="GOC Existing")
    assert created is False
    assert client.pk == existing.pk


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_or_create_creates():
    client, created = await Client.objects.aupdate_or_create(
        name="UOC New", defaults={"name": "UOC New"}
    )
    assert created is True
    assert client.pk is not None


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_or_create_updates():
    existing = await Client.objects.acreate(name="UOC Original")
    client, created = await Client.objects.aupdate_or_create(
        pk=existing.pk, defaults={"name": "UOC Updated"}
    )
    assert created is False
    assert client.name == "UOC Updated"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aearliest():
    await Client.objects.abulk_create([
        Client(name="Early A"),
        Client(name="Early B"),
        Client(name="Early C"),
    ])
    earliest = await Client.objects.filter(name__startswith="Early ").aearliest("pk")
    assert earliest.name == "Early A"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_alatest():
    await Client.objects.abulk_create([
        Client(name="Late A"),
        Client(name="Late B"),
        Client(name="Late C"),
    ])
    latest = await Client.objects.filter(name__startswith="Late ").alatest("pk")
    assert latest.name == "Late C"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_ain_bulk():
    clients = await Client.objects.abulk_create([
        Client(name="Bulk IB A"),
        Client(name="Bulk IB B"),
    ])
    pks = [c.pk for c in clients]
    result = await Client.objects.ain_bulk(pks)
    assert set(result.keys()) == set(pks)
    assert result[pks[0]].name == "Bulk IB A"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acontains():
    client = await Client.objects.acreate(name="Contains Me")
    other = Client(pk=999999)
    assert await Client.objects.acontains(client) is True
    assert await Client.objects.acontains(other) is False


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_adelete_cascade():
    """Deleting a client cascades to its invoices."""
    client = await Client.objects.acreate(name="Cascade Client")
    await Invoice.objects.acreate(
        client=client, reference="INV-C1", total=Decimal("50.00")
    )
    await Invoice.objects.acreate(
        client=client, reference="INV-C2", total=Decimal("75.00")
    )
    deleted_count, breakdown = await Client.objects.filter(pk=client.pk).adelete()
    assert deleted_count == 3  # 1 client + 2 invoices
    assert not await Invoice.objects.filter(client_id=client.pk).aexists()
