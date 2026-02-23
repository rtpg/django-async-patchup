#!/usr/bin/env python3
"""Tests for django_async_patchup overrides to increase coverage."""
import pytest
from asgiref.sync import sync_to_async
from biz.models import Client, Invoice
from decimal import Decimal

import django_async_patchup

django_async_patchup.setup()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_queryset_aiter():
    """Test QuerySet.__aiter__() override via async for."""
    # Create some test data using sync method wrapped for async context
    await sync_to_async(Client.objects.create)(name="AIter_Client_A")
    await sync_to_async(Client.objects.create)(name="AIter_Client_B")

    # Test async iteration (filter to avoid contamination from other test data)
    clients = [c async for c in Client.objects.filter(name__startswith="AIter_Client_")]
    assert len(clients) == 2
    names = {c.name for c in clients}
    assert names == {"AIter_Client_A", "AIter_Client_B"}


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_queryset_acount():
    """Test QuerySet.acount() override."""
    # Get initial count
    initial_count = await Client.objects.acount()

    # Create test data
    await sync_to_async(Client.objects.create)(name="Client A")
    await sync_to_async(Client.objects.create)(name="Client B")
    await sync_to_async(Client.objects.create)(name="Client C")

    # Test async count
    count = await Client.objects.acount()
    assert count == initial_count + 3


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_queryset_aexists():
    """Test QuerySet.aexists() override."""
    # Test with empty filter - should exist after creating
    await sync_to_async(Client.objects.create)(name="Exists Test")

    exists = await Client.objects.filter(name="Exists Test").aexists()
    assert exists is True

    not_exists = await Client.objects.filter(name="Does Not Exist XYZ123").aexists()
    assert not_exists is False


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_queryset_afirst_alast():
    """Test QuerySet.afirst() and alast() overrides."""
    # Create ordered test data with unique prefix
    await sync_to_async(Client.objects.create)(name="FL_Test_AAA First")
    await sync_to_async(Client.objects.create)(name="FL_Test_ZZZ Last")

    # Test afirst with filter
    qs = Client.objects.filter(name__startswith="FL_Test_").order_by("name")
    first = await qs.afirst()
    assert first is not None
    assert first.name == "FL_Test_AAA First"

    # Test alast with filter
    last = await qs.alast()
    assert last is not None
    assert last.name == "FL_Test_ZZZ Last"

    # Test on empty queryset
    none_result = await Client.objects.filter(name="NonExistent XYZ").afirst()
    assert none_result is None


@pytest.mark.todo("get async fallbacks working")
@pytest.mark.asyncio
@pytest.mark.django_db
async def test_queryset_aaggregate():
    """Test QuerySet.aaggregate() override."""
    from django.db.models import Sum

    # Create test data with invoices
    client = await sync_to_async(Client.objects.create)(name="Aggregate Client")
    await sync_to_async(Invoice.objects.create)(
        client=client, reference="INV-001", total=Decimal("100.00")
    )
    await sync_to_async(Invoice.objects.create)(
        client=client, reference="INV-002", total=Decimal("200.00")
    )

    # Test async aggregate
    result = await Invoice.objects.filter(client=client).aaggregate(
        total_sum=Sum("total")
    )
    assert result["total_sum"] == Decimal("300.00")


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_queryset_asave():
    # _Very_ simple save coverage test
    client = Client(name="Hello There")
    await client.asave()
    assert client.pk is not None
    assert (await Client.objects.aget(pk=client.pk)).name == "Hello There"
