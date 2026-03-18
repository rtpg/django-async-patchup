#!/usr/bin/env python3
"""Extended coverage tests for django_async_patchup."""
import pytest
from decimal import Decimal
from asgiref.sync import sync_to_async
from biz.models import Client, Invoice, Person, Employee
from django.db.models import Count

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


# ---------------------------------------------------------------------------
# hello() — __init__.py line 27
# ---------------------------------------------------------------------------

def test_hello_runs(capsys):
    """hello() prints Hi — covers __init__.py line 27."""
    django_async_patchup.hello()
    captured = capsys.readouterr()
    assert "Hi" in captured.out


# ---------------------------------------------------------------------------
# acount() with pre-evaluated queryset — query.py cached path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acount_uses_result_cache_when_already_evaluated():
    """acount() returns len(_result_cache) when queryset is already evaluated."""
    await Client.objects.acreate(name="CacheCount_A")
    await Client.objects.acreate(name="CacheCount_B")
    qs = Client.objects.filter(name__startswith="CacheCount_")
    # Pre-evaluate to populate _result_cache
    await qs._afetch_all()
    assert qs._result_cache is not None
    # acount should now use the cache, not hit the DB
    count = await qs.acount()
    assert count == 2


# ---------------------------------------------------------------------------
# abulk_create on a multi-table-inherited model — query.py lines 316-317
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_create_rejects_multi_table_inherited_model():
    """abulk_create raises ValueError for MTI models (can't bulk create across tables)."""
    with pytest.raises(ValueError, match="multi-table inherited"):
        await Employee.objects.abulk_create([Employee(first_name="MTI_A", department="Eng")])


# ---------------------------------------------------------------------------
# aget() with combinator + filter args — query.py lines 210-213
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aget_raises_on_combinator_with_filters():
    """aget(*args) on a union queryset raises NotSupportedError."""
    from django.db.utils import NotSupportedError
    qs1 = Client.objects.filter(name="GetCombinator_X")
    qs2 = Client.objects.filter(name="GetCombinator_Y")
    combined = qs1.union(qs2)
    with pytest.raises(NotSupportedError, match="union"):
        await combined.aget(name="GetCombinator_X")


# ===========================================================================
# registry.py — lines 64, 76-84
# ===========================================================================

def test_sync_methods_returns_list():
    from django_async_patchup.registry import sync_methods
    result = sync_methods()
    assert isinstance(result, list)


def test_from_codegen_registers_and_returns_function():
    from django_async_patchup.registry import from_codegen, _registry
    sentinel = object()
    len_before = len(_registry)

    @from_codegen(original=sentinel)
    def sample_func():
        pass

    try:
        assert len(_registry) == len_before + 1
        item = _registry[-1]
        assert item.label == "from_codegen"
        assert item.original_copy is sentinel
        assert item.our_copy is sample_func
    finally:
        _registry.pop()


# ===========================================================================
# transaction.py — lines 1-27
# ===========================================================================

@pytest.mark.asyncio
async def test_mark_for_rollback_on_error_no_exception():
    from django_async_patchup.db.models.transaction import MarkForRollbackOnError
    m = MarkForRollbackOnError(using="default")
    async with m:
        pass  # __aexit__ with exc_val=None is a no-op


def test_mark_for_rollback_sync_enter_exit():
    from django_async_patchup.db.models.transaction import MarkForRollbackOnError
    m = MarkForRollbackOnError(using="default")
    with m:
        pass  # __enter__ returns self, __exit__ with no exc does nothing


def test_amark_for_rollback_on_error_factory():
    from django_async_patchup.db.models.transaction import (
        amark_for_rollback_on_error,
        MarkForRollbackOnError,
    )
    m = amark_for_rollback_on_error(using="default")
    assert isinstance(m, MarkForRollbackOnError)


# ===========================================================================
# db/__init__.py — lines 14-60, 70-88
# ===========================================================================

def test_modify_cxn_depth():
    from django_async_patchup.db import modify_cxn_depth, new_connection_block_depth
    original = new_connection_block_depth.value
    modify_cxn_depth(lambda v: v + 1)
    assert new_connection_block_depth.value == original + 1
    modify_cxn_depth(lambda v: v - 1)  # restore


def test_is_commit_allowed_returns_bool():
    from django_async_patchup.db import is_commit_allowed
    assert isinstance(is_commit_allowed(), bool)


def test_set_async_db_commit_permission():
    from django_async_patchup.db import set_async_db_commit_permission, is_commit_allowed
    with set_async_db_commit_permission(True):
        assert is_commit_allowed() is True
    with set_async_db_commit_permission(False):
        assert is_commit_allowed() is False


def test_allow_async_db_commits():
    from django_async_patchup.db import allow_async_db_commits, is_commit_allowed
    with allow_async_db_commits():
        assert is_commit_allowed() is True


def test_block_async_db_commits():
    from django_async_patchup.db import block_async_db_commits, is_commit_allowed
    with block_async_db_commits():
        assert is_commit_allowed() is False


def test_new_connection_init_with_force_rollback():
    from django_async_patchup.db import new_connection
    conn = new_connection("default", force_rollback=True)
    assert conn.force_rollback is True


def test_new_connection_init_commits_blocked_raises():
    from django_async_patchup.db import new_connection, block_async_db_commits
    with block_async_db_commits():
        with pytest.raises(ValueError, match="blocked"):
            new_connection("default")


def test_new_connection_sync_enter_exit():
    from django_async_patchup.db import new_connection
    conn = new_connection("default", force_rollback=True)
    with conn:
        pass  # __enter__ returns None (force_rollback=True path), __exit__ is no-op


# ===========================================================================
# aget_or_create (query.py ~451-475)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aget_or_create_creates_new_object():
    obj, created = await Client.objects.aget_or_create(name="GetOrCreate_NewXYZ")
    assert created is True
    assert obj.pk is not None


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aget_or_create_returns_existing():
    await Client.objects.acreate(name="GetOrCreate_ExistingXYZ")
    obj, created = await Client.objects.aget_or_create(name="GetOrCreate_ExistingXYZ")
    assert created is False


# ===========================================================================
# aupdate_or_create (query.py ~479-531)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_or_create_creates():
    obj, created = await Client.objects.aupdate_or_create(
        name="UOC_NewXYZ", defaults={"metadata": {"x": 1}}
    )
    assert created is True
    assert obj.metadata == {"x": 1}


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_or_create_updates_existing():
    await Client.objects.acreate(name="UOC_ExistingXYZ")
    obj, created = await Client.objects.aupdate_or_create(
        name="UOC_ExistingXYZ", defaults={"metadata": {"updated": True}}
    )
    assert created is False
    assert obj.metadata == {"updated": True}


# ===========================================================================
# aearliest / alatest (query.py ~535-578)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aearliest_by_field():
    await Client.objects.acreate(name="EarlyTest_Z")
    await Client.objects.acreate(name="EarlyTest_A")
    obj = await Client.objects.filter(name__startswith="EarlyTest_").aearliest("name")
    assert obj.name == "EarlyTest_A"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_alatest_by_field():
    await Client.objects.acreate(name="LateTest_A")
    await Client.objects.acreate(name="LateTest_Z")
    obj = await Client.objects.filter(name__startswith="LateTest_").alatest("name")
    assert obj.name == "LateTest_Z"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aearliest_sliced_raises():
    with pytest.raises(TypeError, match="slice"):
        await Client.objects.all()[:3].aearliest("name")


# ===========================================================================
# ain_bulk (query.py ~608-659)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_ain_bulk_empty_list():
    result = await Client.objects.ain_bulk([])
    assert result == {}


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_ain_bulk_with_ids():
    c1 = await Client.objects.acreate(name="InBulk_AXY")
    c2 = await Client.objects.acreate(name="InBulk_BXY")
    result = await Client.objects.ain_bulk([c1.pk, c2.pk])
    assert result[c1.pk].name == "InBulk_AXY"
    assert result[c2.pk].name == "InBulk_BXY"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_ain_bulk_no_id_list_returns_all():
    await Client.objects.acreate(name="InBulkAll_XY")
    result = await Client.objects.filter(name__startswith="InBulkAll_").ain_bulk()
    assert len(result) >= 1


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_ain_bulk_non_unique_field_raises():
    with pytest.raises(ValueError, match="unique field"):
        await Client.objects.ain_bulk(field_name="name")


# ===========================================================================
# adelete error cases (query.py ~661-693)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_adelete_sliced_raises():
    with pytest.raises(TypeError, match="limit"):
        await Client.objects.all()[:3].adelete()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_adelete_distinct_fields_raises():
    with pytest.raises(TypeError, match="distinct"):
        await Client.objects.distinct("name").adelete()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_adelete_values_raises():
    with pytest.raises(TypeError, match="values"):
        await Client.objects.values("name").adelete()


# ===========================================================================
# aupdate error cases (query.py ~710-721)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_sliced_raises():
    with pytest.raises(TypeError, match="slice"):
        await Client.objects.all()[:3].aupdate(name="X")


# ===========================================================================
# aexists with result cache / acontains (query.py ~785-815)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aexists_with_nonempty_result_cache():
    await Client.objects.acreate(name="ExistsCache_XY")
    qs = Client.objects.filter(name__startswith="ExistsCache_XY")
    await qs._afetch_all()
    assert qs._result_cache is not None
    assert await qs.aexists() is True


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aexists_with_empty_result_cache():
    qs = Client.objects.filter(name="ExistsCache_NeverExists_XYZQ")
    await qs._afetch_all()
    assert qs._result_cache == []
    assert await qs.aexists() is False


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acontains_returns_false_for_wrong_model():
    client = await Client.objects.acreate(name="ContainsWrong_Client")
    inv = await Invoice.objects.acreate(
        client=client, reference="CW-INV-001", total=Decimal("1.00")
    )
    result = await Client.objects.acontains(inv)
    assert result is False


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acontains_values_queryset_raises():
    client = await Client.objects.acreate(name="ContainsVals_X")
    with pytest.raises(TypeError, match="values"):
        await Client.objects.values("name").acontains(client)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acontains_unsaved_object_raises():
    unsaved = Client(name="ContainsUnsaved")
    with pytest.raises(ValueError, match="unsaved"):
        await Client.objects.acontains(unsaved)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acontains_with_result_cache():
    c = await Client.objects.acreate(name="ContainsCache_XY")
    qs = Client.objects.filter(name__startswith="ContainsCache_XY")
    await qs._afetch_all()
    assert qs._result_cache is not None
    assert await qs.acontains(c) is True


# ===========================================================================
# asave error paths (models/__init__.py lines 71, 257)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_asave_invalid_update_fields_raises():
    c = await Client.objects.acreate(name="BadUpdateField")
    with pytest.raises(ValueError, match="do not exist"):
        await c.asave(update_fields=["nonexistent_field"])


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_asave_force_update_no_pk_raises():
    c = Client(name="ForceUpdateNoPK")
    with pytest.raises(ValueError, match="Cannot force an update in save"):
        await c.asave(force_update=True)


# ===========================================================================
# abulk_update error paths (query.py ~395-410)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_update_obj_without_pk_raises():
    unsaved = Client(name="NoPKBulk")
    with pytest.raises(ValueError, match="primary key"):
        await Client.objects.abulk_update([unsaved], fields=["name"])


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_update_pk_field_raises():
    c = await Client.objects.acreate(name="PKFieldBulk")
    with pytest.raises(ValueError, match="primary key fields"):
        await Client.objects.abulk_update([c], fields=["id"])


# ===========================================================================
# abulk_create edge cases (query.py ~318-356)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_create_empty_list():
    result = await Client.objects.abulk_create([])
    assert result == []


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_create_with_explicit_pk():
    explicit_pk = 9876543
    await Client.objects.filter(pk=explicit_pk).adelete()
    try:
        objs = await Client.objects.abulk_create([Client(pk=explicit_pk, name="ExplicitPK_Test")])
        assert len(objs) == 1
    finally:
        await Client.objects.filter(pk=explicit_pk).adelete()


# ===========================================================================
# _afetch_all when result_cache is already set — query.py branch 18->23
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_afetch_all_skips_population_when_cache_already_set():
    """Second call to _afetch_all skips population since _result_cache is not None."""
    await Client.objects.acreate(name="FetchCacheSkip_A")
    qs = Client.objects.filter(name__startswith="FetchCacheSkip_")
    # First call populates the cache
    await qs._afetch_all()
    assert qs._result_cache is not None
    original_cache = qs._result_cache[:]
    # Second call: _result_cache is not None → branch 18->23 (skip the population)
    await qs._afetch_all()
    assert qs._result_cache == original_cache


# ===========================================================================
# QuerySetOverrides._fetch_then_len static method — query.py lines 37-38
# ===========================================================================

@pytest.mark.django_db
def test_queryset_overrides_fetch_then_len_static():
    """_fetch_then_len(qs) fetches synchronously and returns len of cache."""
    from django_async_patchup.db.models.query import QuerySetOverrides
    Client.objects.create(name="FetchThenLen_X")
    qs = Client.objects.filter(name__startswith="FetchThenLen_")
    result = QuerySetOverrides._fetch_then_len(qs)
    assert isinstance(result, int)
    assert result >= 1


# ===========================================================================
# _aearliest with Meta.get_latest_by string — query.py line 546
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aearliest_no_fields_uses_meta_get_latest_by_string():
    """_aearliest() with no fields and Meta.get_latest_by as a string covers line 546."""
    from biz.models import Client

    class ClientByName(Client):
        class Meta:
            proxy = True
            app_label = "biz"
            get_latest_by = "name"

    await sync_to_async(Client.objects.create)(name="MetaEarliest_Z")
    await sync_to_async(Client.objects.create)(name="MetaEarliest_A")
    obj = await ClientByName.objects.filter(name__startswith="MetaEarliest_").aearliest()
    assert obj.name == "MetaEarliest_A"


# ===========================================================================
# adelete() actual deletion — covers deletion.py core paths
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_adelete_actually_deletes_objects():
    """Calling adelete() on a queryset deletes the matched objects."""
    await Client.objects.acreate(name="ActualDelete_A")
    await Client.objects.acreate(name="ActualDelete_B")
    count, details = await Client.objects.filter(name__startswith="ActualDelete_").adelete()
    assert count >= 2
    assert not await Client.objects.filter(name__startswith="ActualDelete_").aexists()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_adelete_with_cascade_deletes_related():
    """adelete() on a Client with Invoices triggers cascade delete."""
    client = await Client.objects.acreate(name="CascadeDelete_Client")
    await Invoice.objects.acreate(client=client, reference="CDE-001", total=Decimal("1.00"))
    await Invoice.objects.acreate(client=client, reference="CDE-002", total=Decimal("2.00"))
    count, details = await Client.objects.filter(pk=client.pk).adelete()
    # Client + 2 Invoices deleted
    assert count >= 3
    assert not await Invoice.objects.filter(client=client).aexists()


# ===========================================================================
# aupdate() with order_by — query.py lines 728-743 (order_by inlining loop)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_with_ascending_ordering_covers_order_loop():
    """aupdate on a queryset with order_by covers the order_by annotation-inline loop."""
    await Client.objects.acreate(name="OrdUpd_A")
    await Client.objects.acreate(name="OrdUpd_B")
    count = await (
        Client.objects.filter(name__startswith="OrdUpd_")
        .order_by("name")
        .aupdate(metadata={"step": 1})
    )
    assert count >= 2


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_with_descending_ordering_covers_desc_branch():
    """aupdate with '-field' ordering covers the descending=True branch (line 732-733)."""
    await Client.objects.acreate(name="OrdUpdDesc_A")
    await Client.objects.acreate(name="OrdUpdDesc_B")
    count = await (
        Client.objects.filter(name__startswith="OrdUpdDesc_")
        .order_by("-name")
        .aupdate(metadata={"step": 2})
    )
    assert count >= 2


# ===========================================================================
# abulk_update on MTI model — query.py line 408 (parent pk_fields loop)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_update_mti_model_covers_parent_pk_fields_loop():
    """abulk_update on Employee (MTI) covers the all_parents loop at line 408."""
    emp = await Employee.objects.acreate(first_name="BulkUpdMTI_Emp", department="Eng")
    emp.department = "Updated_Dept"
    count = await Employee.objects.abulk_update([emp], fields=["department"])
    assert count == 1


# ===========================================================================
# transaction.py MarkForRollbackOnError — lines 20-23
# ===========================================================================

def test_mark_for_rollback_sync_exit_exception_outside_atomic():
    """__exit__ with exception outside any transaction covers the False branch of in_atomic_block."""
    from django_async_patchup.db.models.transaction import MarkForRollbackOnError
    m = MarkForRollbackOnError(using="default")
    exc = ValueError("test error outside atomic")
    # Without @pytest.mark.django_db, connection.in_atomic_block is False → 32->exit branch covered
    result = m.__exit__(type(exc), exc, None)
    assert result is None  # exception is not suppressed


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_mark_for_rollback_async_exit_with_exception():
    """async __aexit__ with exception covers transaction.py lines 24-25."""
    from django_async_patchup.db.models.transaction import MarkForRollbackOnError
    m = MarkForRollbackOnError(using="default")
    exc = ValueError("async error")
    # Covers line 24 (aget_connection) and line 25 (in_atomic_block check)
    await m.__aexit__(type(exc), exc, None)


@pytest.mark.django_db
def test_mark_for_rollback_sync_exit_exception_in_atomic():
    """__exit__ with an exception IN an atomic block covers lines 22-23."""
    from django_async_patchup.db.models.transaction import MarkForRollbackOnError
    from django.db import connection, transaction
    m = MarkForRollbackOnError(using="default")
    exc = ValueError("test error in atomic")
    with transaction.atomic():
        m.__exit__(type(exc), exc, None)
        assert connection.needs_rollback is True
        # Reset to allow clean teardown
        connection.needs_rollback = False


# ===========================================================================
# db/__init__.py line 84 — new_connection.__enter__ raises without force_rollback
# ===========================================================================

def test_new_connection_sync_enter_raises_without_force_rollback():
    """new_connection().__enter__() raises NotSupportedError unless force_rollback=True."""
    from django_async_patchup.db import new_connection, allow_async_db_commits
    from django.db.utils import NotSupportedError
    with allow_async_db_commits():
        conn = new_connection("default", force_rollback=False)
        with pytest.raises(NotSupportedError, match="sync context"):
            conn.__enter__()


# ===========================================================================
# sql/__init__.py lines 86-157 — aget_aggregation subquery path (sliced qs)
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_on_sliced_queryset_uses_subquery():
    """aaggregate() on a sliced queryset uses the AggregateQuery subquery path."""
    await Client.objects.acreate(name="SubqAgg_A")
    await Client.objects.acreate(name="SubqAgg_B")
    await Client.objects.acreate(name="SubqAgg_C")
    result = await Client.objects.filter(name__startswith="SubqAgg_")[:2].aaggregate(
        count=Count("id")
    )
    assert result["count"] == 2


# ===========================================================================
# sql/__init__.py lines 170-174 — aget_aggregation with non-aggregate annotation
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_with_non_aggregate_annotation_hits_inline_path():
    """aaggregate() with a non-aggregate annotation inlines it (lines 170-174)."""
    from django.db.models.functions import Length

    await Client.objects.acreate(name="InlineAgg")
    result = await Client.objects.filter(name="InlineAgg").annotate(
        name_len=Length("name")
    ).aaggregate(count=Count("id"))
    assert result["count"] == 1


# ===========================================================================
# sql/__init__.py 98->135 — distinct inner_query path in aget_aggregation
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_on_distinct_queryset_uses_distinct_subquery_path():
    """Aggregating on a .distinct() queryset; inner_query.distinct=True → branch 98->135."""
    await Client.objects.acreate(name="DistAgg_X")
    await Client.objects.acreate(name="DistAgg_Y")
    result = await Client.objects.filter(
        name__startswith="DistAgg_"
    ).distinct().aaggregate(count=Count("id"))
    assert result["count"] == 2


# ===========================================================================
# sql/__init__.py line 157 — inner_query.select forced when empty
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acount_on_sliced_queryset_forces_inner_select():
    """qs[:3].acount() triggers the inner_query.select assignment (line 157)."""
    for i in range(4):
        await Client.objects.acreate(name=f"SliceCount_{i}")
    result = await Client.objects.filter(name__startswith="SliceCount_")[:3].acount()
    assert result == 3


# ===========================================================================
# compiler.py lines 303-306 — EmptyResultSet in aexecute_sql (MULTI + non-MULTI)
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_on_impossible_filter_hits_empty_result_set():
    """filter(pk__in=[]) raises EmptyResultSet in aexecute_sql; MULTI returns iter([])."""
    results = [obj async for obj in Client.objects.filter(pk__in=[]).aiterator()]
    assert results == []


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aexists_on_impossible_filter_hits_empty_result_set_single():
    """filter(pk__in=[]) raises EmptyResultSet; SINGLE returns None → False."""
    exists = await Client.objects.filter(pk__in=[]).aexists()
    assert exists is False


# ===========================================================================
# deletion.py lines 231-236 — Collector.adelete single-instance fast path
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_adelete_single_instance_fast_path_via_signal():
    """A single-instance delete where queryset can't fast-delete but instance can."""
    from django.db.models.signals import pre_delete

    calls = []

    def _handler(sender, instance, **kwargs):
        calls.append(instance)

    # Connecting pre_delete prevents queryset-level fast delete,
    # forcing the instance through self.data so the single-instance
    # path in Collector.adelete (lines 228-236) is reached.
    pre_delete.connect(_handler, sender=Person)
    try:
        person = await Person.objects.acreate(first_name="FastDeletePerson")
        count, _ = await Person.objects.filter(pk=person.pk).adelete()
        assert count == 1
        assert len(calls) == 1
    finally:
        pre_delete.disconnect(_handler, sender=Person)


# ---------------------------------------------------------------------------
# registry.get_owning_class edge cases (lines 13, 21)
# ---------------------------------------------------------------------------


def test_get_owning_class_with_bound_method():
    """get_owning_class unwraps bound methods via __func__ (covers line 13)."""
    from django_async_patchup.registry import get_owning_class
    from django.db.models import Model

    client = Client()
    # client.save is a bound method → inspect.ismethod returns True
    result = get_owning_class(client.save)
    assert result is Model


def test_get_owning_class_mismatch_returns_none():
    """get_owning_class returns None when the class attr doesn't match func (covers line 21)."""
    import django.db.models.base as base_module
    from django_async_patchup.registry import get_owning_class

    def fake_save():
        pass

    fake_save.__qualname__ = "Model.save"
    fake_save.__module__ = base_module.__name__

    result = get_owning_class(fake_save)
    assert result is None


# ---------------------------------------------------------------------------
# MarkForRollbackOnError.__aexit__ with an exception (transaction.py lines 26-27)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_for_rollback_on_error_aexit_sets_rollback(monkeypatch):
    """MarkForRollbackOnError.__aexit__ marks connection for rollback when an exception occurs in an atomic block."""
    import django_async_patchup.db.models.transaction as txn_mod
    from django_async_patchup.db.models.transaction import MarkForRollbackOnError

    class FakeConn:
        in_atomic_block = True
        needs_rollback = False
        rollback_exc = None

    fake_conn = FakeConn()

    async def fake_aget_connection(using):
        return fake_conn

    monkeypatch.setattr(txn_mod, "aget_connection", fake_aget_connection)

    ctx = MarkForRollbackOnError(using="default")
    exc = ValueError("simulated error")
    await ctx.__aexit__(ValueError, exc, None)
    assert fake_conn.needs_rollback is True
    assert fake_conn.rollback_exc is exc

