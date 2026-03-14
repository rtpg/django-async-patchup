#!/usr/bin/env python3
"""Tests for django_async_patchup overrides to increase coverage."""
import pytest
from asgiref.sync import sync_to_async
from biz.models import Client, Invoice
from decimal import Decimal
from django.db.models import Count, Sum

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


@pytest.mark.skip("get async fallbacks working")
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


# --- aexecute_sql chunked_cursor pathway tests ---
# These exercise SQLCompiler.aexecute_sql with chunked_fetch=True via aiterator(),
# which triggers self.connection.achunked_cursor() (server-side cursor on PostgreSQL)
# and returns an AsyncGenerator from acursor_iter() rather than a pre-fetched list.


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_basic_chunked_cursor():
    """aiterator() uses chunked_fetch=True, exercising the achunked_cursor path."""
    prefix = "ChunkBasic_"
    names = [f"{prefix}{i}" for i in range(5)]
    for name in names:
        await sync_to_async(Client.objects.create)(name=name)

    collected = []
    async for client in Client.objects.filter(name__startswith=prefix).aiterator():
        collected.append(client.name)

    assert sorted(collected) == sorted(names)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_small_chunk_size_forces_multiple_fetchmany():
    """chunk_size=1 forces acursor_iter to call fetchmany() once per row."""
    prefix = "ChunkSmall_"
    names = [f"{prefix}{i}" for i in range(4)]
    for name in names:
        await sync_to_async(Client.objects.create)(name=name)

    collected = []
    async for client in Client.objects.filter(name__startswith=prefix).aiterator(
        chunk_size=1
    ):
        collected.append(client.name)

    assert sorted(collected) == sorted(names)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_chunk_size_spans_multiple_chunks():
    """Dataset larger than chunk_size verifies chunked reads across multiple fetchmany calls."""
    prefix = "ChunkMulti_"
    names = [f"{prefix}{i:02d}" for i in range(7)]
    for name in names:
        await sync_to_async(Client.objects.create)(name=name)

    collected = []
    async for client in Client.objects.filter(name__startswith=prefix).aiterator(
        chunk_size=3
    ):
        collected.append(client.name)

    assert sorted(collected) == sorted(names)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_empty_queryset_chunked_cursor():
    """aiterator() on an empty result set returns no rows (EmptyResultSet or empty fetchmany)."""
    collected = []
    async for client in Client.objects.filter(
        name="__nonexistent_chunked_test__"
    ).aiterator():
        collected.append(client)

    assert collected == []


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_with_ordering_chunked_cursor():
    """Chunked path preserves ORDER BY correctly across chunk boundaries."""
    prefix = "ChunkOrder_"
    names = [f"{prefix}{chr(ord('A') + i)}" for i in range(5)]
    for name in names:
        await sync_to_async(Client.objects.create)(name=name)

    collected = []
    async for client in (
        Client.objects.filter(name__startswith=prefix)
        .order_by("name")
        .aiterator(chunk_size=2)
    ):
        collected.append(client.name)

    assert collected == sorted(names)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_model_subclass_with_save_override_gets_asave():
    """Model overriding save() but not asave() → __init_subclass__ auto-creates asave (line 25)."""
    from django.db import models

    class ClientWithSave(Client):
        save_called = False

        def save(self, *args, **kwargs):
            ClientWithSave.save_called = True
            super().save(*args, **kwargs)

        class Meta:
            proxy = True
            app_label = "biz"

    obj = ClientWithSave(name="SaveOverride")
    await obj.asave()
    assert ClientWithSave.save_called
    assert obj.pk is not None


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_model_subclass_with_asave_override_gets_save():
    """Model overriding asave() but not save() → __init_subclass__ auto-creates save (line 28)."""

    class ClientWithAsave(Client):
        asave_called = False

        async def asave(self, *args, **kwargs):
            ClientWithAsave.asave_called = True
            await super().asave(*args, **kwargs)

        class Meta:
            proxy = True
            app_label = "biz"

    obj = ClientWithAsave(name="AsaveOverride")
    await sync_to_async(obj.save)()
    assert ClientWithAsave.asave_called
    assert obj.pk is not None


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_model_subclass_with_both_overrides_each_called_independently():
    """Model overriding both save and asave — each method is called for its own path, not the other."""

    class ClientWithBoth(Client):
        save_called = False
        asave_called = False

        def save(self, *args, **kwargs):
            ClientWithBoth.save_called = True
            super().save(*args, **kwargs)

        async def asave(self, *args, **kwargs):
            ClientWithBoth.asave_called = True
            await super().asave(*args, **kwargs)

        class Meta:
            proxy = True
            app_label = "biz"

    # sync save: only save_called, not asave_called
    obj1 = ClientWithBoth(name="BothSync")
    await sync_to_async(obj1.save)()
    assert ClientWithBoth.save_called
    assert not ClientWithBoth.asave_called

    # reset
    ClientWithBoth.save_called = False

    # async asave: only asave_called, not save_called
    obj2 = ClientWithBoth(name="BothAsync")
    await obj2.asave()
    assert ClientWithBoth.asave_called
    assert not ClientWithBoth.save_called


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_with_positional_arg_covers_default_alias_path():
    """aaggregate(Count('id')) — positional arg triggers default_alias loop (lines 68-72)."""
    await sync_to_async(Client.objects.create)(name="AggPos_A")
    await sync_to_async(Client.objects.create)(name="AggPos_B")
    result = await Client.objects.filter(name__startswith="AggPos_").aaggregate(Count("id"))
    assert result["id__count"] == 2


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_positional_complex_raises_type_error():
    """aaggregate with a complex expression that has no default_alias raises TypeError (line 71)."""
    from django.db.models import ExpressionWrapper, IntegerField
    expr = ExpressionWrapper(Count("id") + Count("id"), output_field=IntegerField())
    with pytest.raises(TypeError, match="Complex aggregates require an alias"):
        await Client.objects.aaggregate(expr)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_select_related_async_iteration_covers_related_populators():
    """select_related() with async iteration covers rel_populators path (query.py line 165)."""
    client = await sync_to_async(Client.objects.create)(name="SR_Client")
    await sync_to_async(Invoice.objects.create)(
        client=client, reference="SR_INV_001", total=Decimal("99.99")
    )
    invoices = [
        inv
        async for inv in Invoice.objects.select_related("client").filter(
            reference="SR_INV_001"
        )
    ]
    assert len(invoices) == 1
    assert invoices[0].client.name == "SR_Client"
