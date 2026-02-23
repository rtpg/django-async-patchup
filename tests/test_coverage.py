#!/usr/bin/env python3
"""Extended coverage tests for django_async_patchup."""
import pytest
from decimal import Decimal
from biz.models import Client, Invoice, Person, Employee

import django_async_patchup

django_async_patchup.setup()


# ---------------------------------------------------------------------------
# asave edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_asave_with_update_fields():
    """asave(update_fields=[...]) only updates specified fields."""
    client = await Client.objects.acreate(name="Before Update Fields")
    client.name = "After Update Fields"
    await client.asave(update_fields=["name"])

    refreshed = await Client.objects.aget(pk=client.pk)
    assert refreshed.name == "After Update Fields"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_asave_with_empty_update_fields_is_noop():
    """asave(update_fields=[]) returns early without touching the DB."""
    client = await Client.objects.acreate(name="Noop Save")
    client.name = "Changed But Not Saved"
    await client.asave(update_fields=[])  # should be a no-op

    refreshed = await Client.objects.aget(pk=client.pk)
    assert refreshed.name == "Noop Save"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_asave_force_insert_and_force_update_raises():
    """Both force_insert=True and force_update=True together must raise."""
    client = Client(name="Error Case")
    with pytest.raises(ValueError, match="Cannot force both insert and updating"):
        await client.asave(force_insert=True, force_update=True)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_asave_force_update_on_existing():
    """force_update=True on an existing record should succeed."""
    client = await Client.objects.acreate(name="Force Update Target")
    client.name = "Force Updated"
    await client.asave(force_update=True)

    refreshed = await Client.objects.aget(pk=client.pk)
    assert refreshed.name == "Force Updated"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_asave_update_path():
    """Saving an already-persisted object goes through the UPDATE path."""
    client = await Client.objects.acreate(name="Update Path")
    original_pk = client.pk
    client.name = "Updated Via asave"
    await client.asave()

    # PK should not change (it was an UPDATE)
    assert client.pk == original_pk
    refreshed = await Client.objects.aget(pk=client.pk)
    assert refreshed.name == "Updated Via asave"


# ---------------------------------------------------------------------------
# abulk_create edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_create_empty_list():
    """abulk_create([]) returns immediately with the same empty list."""
    result = await Client.objects.abulk_create([])
    assert result == []


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_create_with_explicit_pks():
    """Objects with PKs already set go through the objs_with_pk branch."""
    # Clean up any pre-existing rows from earlier runs.
    await Client.objects.filter(pk__in=[9_000_001, 9_000_002]).adelete()

    clients = await Client.objects.abulk_create(
        [
            Client(pk=9_000_001, name="Explicit PK A"),
            Client(pk=9_000_002, name="Explicit PK B"),
        ]
    )
    assert len(clients) == 2
    assert await Client.objects.filter(pk__in=[9_000_001, 9_000_002]).acount() == 2


# ---------------------------------------------------------------------------
# abulk_update edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_update_empty_objs():
    """abulk_update with no objects returns 0 immediately."""
    result = await Client.objects.abulk_update([], fields=["name"])
    assert result == 0


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_update_with_batch_size():
    """abulk_update with explicit batch_size parameter."""
    clients = await Client.objects.abulk_create(
        [Client(name="BS Before A"), Client(name="BS Before B"), Client(name="BS Before C")]
    )
    for c in clients:
        c.name = c.name.replace("Before", "After")
    updated = await Client.objects.abulk_update(clients, fields=["name"], batch_size=2)
    assert updated == 3


# ---------------------------------------------------------------------------
# ain_bulk edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_ain_bulk_empty_list():
    """ain_bulk([]) returns an empty dict immediately."""
    result = await Client.objects.ain_bulk([])
    assert result == {}


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_ain_bulk_no_id_list():
    """ain_bulk() with no id_list returns all objects in the queryset."""
    clients = await Client.objects.abulk_create(
        [Client(name="NID A"), Client(name="NID B")]
    )
    pks = {c.pk for c in clients}
    result = await Client.objects.filter(pk__in=pks).ain_bulk()
    assert set(result.keys()) == pks
    assert all(isinstance(v, Client) for v in result.values())


# ---------------------------------------------------------------------------
# aupdate_or_create with create_defaults
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_or_create_with_create_defaults():
    """create_defaults are used when creating; defaults when updating."""
    await Client.objects.filter(pk=9_999_999).adelete()  # clean up

    obj, created = await Client.objects.aupdate_or_create(
        pk=9_999_999,
        create_defaults={"name": "Created Name"},
        defaults={"name": "Updated Name"},
    )
    assert created is True
    assert obj.name == "Created Name"

    obj2, created2 = await Client.objects.aupdate_or_create(
        pk=9_999_999,
        create_defaults={"name": "Would Not Be Used"},
        defaults={"name": "Updated Name"},
    )
    assert created2 is False
    assert obj2.name == "Updated Name"


# ---------------------------------------------------------------------------
# aexists / acount from result cache
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aexists_uses_result_cache():
    """When result_cache is populated, aexists() returns without a DB query."""
    client = await Client.objects.acreate(name="Cache Exists Test")
    qs = Client.objects.filter(pk=client.pk)
    # Explicitly populate the cache
    await qs._afetch_all()
    assert qs._result_cache is not None
    # Should return True from cache, not DB
    assert await qs.aexists() is True


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acount_uses_result_cache():
    """When result_cache is populated, acount() uses len(cache)."""
    client = await Client.objects.acreate(name="Cache Count Test")
    qs = Client.objects.filter(pk=client.pk)
    await qs._afetch_all()
    assert qs._result_cache is not None
    assert await qs.acount() == 1


# ---------------------------------------------------------------------------
# aaggregate additional coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_positional_and_keyword_args():
    """aaggregate with both positional and keyword expressions."""
    from django.db.models import Sum, Count

    client = await Client.objects.acreate(name="Agg Coverage Client")
    await Invoice.objects.acreate(
        client=client, reference="AGG-1", total=Decimal("150.00")
    )
    await Invoice.objects.acreate(
        client=client, reference="AGG-2", total=Decimal("250.00")
    )

    # Keyword arg form
    result = await Invoice.objects.filter(client=client).aaggregate(
        total_sum=Sum("total"), num=Count("id")
    )
    assert result["total_sum"] == Decimal("400.00")
    assert result["num"] == 2


# ---------------------------------------------------------------------------
# Multi-table inheritance: _asave_parents coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_mti_asave_creates_both_rows():
    """Employee.asave() creates a Person row first via _asave_parents."""
    emp = Employee(first_name="Alice", department="Engineering")
    await emp.asave()

    assert emp.pk is not None
    # Both the parent (Person) and child (Employee) rows should exist.
    person = await Person.objects.aget(pk=emp.pk)
    assert person.first_name == "Alice"
    assert await Employee.objects.filter(pk=emp.pk, department="Engineering").aexists()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_mti_acreate():
    """Employee.objects.acreate() exercises MTI save path end-to-end."""
    emp = await Employee.objects.acreate(first_name="Bob", department="Design")
    assert emp.pk is not None
    assert emp.first_name == "Bob"
    assert emp.department == "Design"

    # Can fetch via both parent and child managers.
    as_person = await Person.objects.aget(pk=emp.pk)
    assert as_person.first_name == "Bob"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_mti_asave_update():
    """Updating an Employee (already persisted) goes through UPDATE for both tables."""
    emp = await Employee.objects.acreate(first_name="Carol", department="HR")
    emp.first_name = "Caroline"
    emp.department = "People Ops"
    await emp.asave()

    refreshed = await Employee.objects.aget(pk=emp.pk)
    assert refreshed.first_name == "Caroline"
    assert refreshed.department == "People Ops"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_mti_adelete():
    """Deleting an Employee cascades to its Person row."""
    emp = await Employee.objects.acreate(first_name="Dave", department="Sales")
    pk = emp.pk

    count, _ = await Employee.objects.filter(pk=pk).adelete()
    assert count >= 1  # 1 Employee + 1 Person at minimum
    assert not await Employee.objects.filter(pk=pk).aexists()
    assert not await Person.objects.filter(pk=pk).aexists()


# ---------------------------------------------------------------------------
# acontains edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acontains_wrong_model_type():
    """acontains returns False when obj is a different model type."""
    client = await Client.objects.acreate(name="Contains Wrong Type")
    invoice = Invoice(pk=client.pk)  # Same pk, different model
    result = await Client.objects.acontains(invoice)
    assert result is False


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acontains_non_model_raises_typeerror():
    """acontains raises TypeError when passed a non-model object."""
    with pytest.raises(TypeError, match="must be a model instance"):
        await Client.objects.acontains("not a model")


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acontains_unsaved_object_raises_valueerror():
    """acontains raises ValueError when obj has no pk set."""
    with pytest.raises(ValueError, match="unsaved"):
        await Client.objects.acontains(Client(name="No PK"))


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acontains_uses_result_cache():
    """acontains short-circuits to cache lookup when cache is populated."""
    client = await Client.objects.acreate(name="Cache Contains")
    qs = Client.objects.filter(pk=client.pk)
    await qs._afetch_all()
    assert qs._result_cache is not None
    # Uses cache path (obj in self._result_cache)
    assert await qs.acontains(client) is True
    # A different client is not in the cache
    other = Client(pk=client.pk + 999_999)
    assert await qs.acontains(other) is False


# ---------------------------------------------------------------------------
# prefetch_related coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aprefetch_related_objects():
    """Using prefetch_related with async iteration exercises _aprefetch_related_objects."""
    client = await Client.objects.acreate(name="Prefetch Client")
    await Invoice.objects.acreate(
        client=client, reference="PF-1", total=Decimal("10.00")
    )
    await Invoice.objects.acreate(
        client=client, reference="PF-2", total=Decimal("20.00")
    )

    qs = Client.objects.filter(pk=client.pk).prefetch_related("invoices")
    clients = [c async for c in qs]
    assert len(clients) == 1
    # After async iteration with prefetch, invoices are cached synchronously
    assert len(list(clients[0].invoices.all())) == 2


# ---------------------------------------------------------------------------
# Fast single-object delete path (lines 231-236 in deletion.py)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_adelete_single_invoice_fast_path():
    """Deleting a single Invoice (no reverse FKs) uses the fast-delete path."""
    client = await Client.objects.acreate(name="Fast Delete Invoice Client")
    invoice = await Invoice.objects.acreate(
        client=client, reference="FD-INV-1", total=Decimal("5.00")
    )
    pk = invoice.pk

    count, breakdown = await Invoice.objects.filter(pk=pk).adelete()
    assert count == 1
    assert not await Invoice.objects.filter(pk=pk).aexists()


# ---------------------------------------------------------------------------
# aget_or_create with defaults dict
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aget_or_create_with_defaults():
    """aget_or_create uses defaults when creating a new object."""
    # Use a unique lookup that won't exist; defaults should be merged.
    name = "GOC Defaults Test"
    await Client.objects.filter(name=name).adelete()  # clean up
    client, created = await Client.objects.aget_or_create(name=name)
    assert created is True
    assert client.name == name

    # Second call: same lookup, object exists, should not create.
    client2, created2 = await Client.objects.aget_or_create(name=name)
    assert created2 is False
    assert client2.pk == client.pk


# ---------------------------------------------------------------------------
# aaggregate with distinct_fields raises NotImplementedError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_distinct_fields_raises():
    """aaggregate raises NotImplementedError when distinct fields are set."""
    from django.db.models import Count

    with pytest.raises(NotImplementedError, match="aggregate.*distinct"):
        await Client.objects.distinct("name").aaggregate(count=Count("id"))


# ---------------------------------------------------------------------------
# afirst / alast ordering edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_alast_on_unordered_queryset():
    """alast() without explicit ordering applies '-pk' ordering."""
    client = await Client.objects.acreate(name="Alast Unordered")
    # Filter without order_by → unordered queryset
    last = await Client.objects.filter(pk=client.pk).alast()
    assert last is not None
    assert last.pk == client.pk


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_alast_on_empty_queryset_returns_none():
    """alast() returns None when the queryset is empty."""
    result = await Client.objects.filter(name="___nonexistent_alast___").alast()
    assert result is None


# ---------------------------------------------------------------------------
# aearliest / alatest error paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aearliest_no_args_no_meta_raises():
    """aearliest() with no fields and no get_latest_by raises ValueError."""
    with pytest.raises(ValueError, match="earliest.*latest.*require"):
        await Client.objects.aearliest()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aearliest_on_sliced_queryset_raises():
    """aearliest() on a sliced queryset raises TypeError."""
    with pytest.raises(TypeError, match="slice"):
        await Client.objects.all()[:5].aearliest("pk")


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_alatest_on_sliced_queryset_raises():
    """alatest() on a sliced queryset raises TypeError."""
    with pytest.raises(TypeError, match="slice"):
        await Client.objects.all()[:5].alatest("pk")


# ---------------------------------------------------------------------------
# abulk_create with batch_size validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_create_invalid_batch_size_raises():
    """abulk_create raises ValueError for batch_size <= 0."""
    with pytest.raises(ValueError, match="Batch size must be a positive integer"):
        await Client.objects.abulk_create([Client(name="X")], batch_size=0)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_update_invalid_batch_size_raises():
    """abulk_update raises ValueError for batch_size <= 0."""
    client = await Client.objects.acreate(name="BulkUpd Invalid BatchSize")
    with pytest.raises(ValueError, match="Batch size must be a positive integer"):
        await Client.objects.abulk_update([client], fields=["name"], batch_size=-1)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_update_no_fields_raises():
    """abulk_update raises ValueError when no fields given."""
    client = await Client.objects.acreate(name="BulkUpd No Fields")
    with pytest.raises(ValueError, match="Field names must be given"):
        await Client.objects.abulk_update([client], fields=[])
