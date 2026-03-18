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
async def test_ain_bulk_sliced_queryset_raises():
    """ain_bulk() on a sliced queryset raises TypeError (query.py line 621)."""
    with pytest.raises(TypeError, match="Cannot use 'limit' or 'offset' with in_bulk"):
        await Client.objects.all()[0:5].ain_bulk()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_ain_bulk_values_queryset_raises():
    """ain_bulk() on a values() queryset raises TypeError (query.py line 623)."""
    with pytest.raises(TypeError, match="in_bulk\\(\\) cannot be used with values"):
        await Client.objects.values("name").ain_bulk()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_no_args_returns_empty_dict():
    """aaggregate() with no args returns {} (sql/__init__.py line 18)."""
    result = await Client.objects.aaggregate()
    assert result == {}


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


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_update_non_concrete_field_raises():
    """abulk_update() with a non-concrete field (reverse FK) raises ValueError (query.py line 405)."""
    client = await Client.objects.acreate(name="BulkUpdateConcrete", metadata={})
    with pytest.raises(ValueError, match="bulk_update\\(\\) can only be used with concrete fields"):
        await Client.objects.abulk_update([client], ["invoices"])


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_aggregate_annotation_order_by_raises():
    """aupdate with an aggregate annotation in order_by raises FieldError (query.py line 735-736)."""
    from django.core.exceptions import FieldError

    await Client.objects.acreate(name="AggOrd", metadata={})
    with pytest.raises(FieldError, match="Cannot update when ordering by an aggregate"):
        await (
            Client.objects.annotate(cnt=Count("id"))
            .order_by("cnt")
            .filter(name="AggOrd")
            .aupdate(metadata={"updated": True})
        )


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_non_aggregate_annotation_order_by_inlines():
    """aupdate with a non-aggregate annotation in order_by inlines it (query.py line 741)."""
    from django.db.models.functions import Length

    await Client.objects.acreate(name="InlineAnno", metadata={})
    count = await (
        Client.objects.annotate(name_len=Length("name"))
        .order_by("name_len")
        .filter(name="InlineAnno")
        .aupdate(metadata={"inlined": True})
    )
    assert count == 1


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_update_with_f_expression_covers_resolve_expression_branch():
    """
    abulk_update() where one of the field values has resolve_expression (F expression).
    Covers the 432->434 branch in query.py (hasattr(attr, 'resolve_expression') is True,
    so the if-not branch is skipped).
    """
    from django.db.models import F

    client = await Client.objects.acreate(name="FExpr_Client")
    invoice = await Invoice.objects.acreate(
        client=client, reference="FE-001", total=Decimal("10.00")
    )

    # Set total to an F expression so attr has resolve_expression
    invoice.total = F("total")
    rows = await Invoice.objects.abulk_update([invoice], ["total"])
    assert rows == 1


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_update_empty_objs_returns_zero():
    """abulk_update() on an empty list returns 0 (query.py line 412)."""
    rows = await Client.objects.abulk_update([], ["name"])
    assert rows == 0


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_collector_aadd_duplicate_skips_existing(transactional_db):
    """Second aadd() call with same object takes False branch of 'if obj not in instances' (deletion.py 49->48)."""
    from django.db.models.deletion import Collector

    client = await Client.objects.acreate(name="DupCollector")
    collector = Collector(using="default")

    result1 = await collector.aadd([client])
    assert result1 == [client]

    result2 = await collector.aadd([client])
    assert result2 == []


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acreate_with_reverse_one_to_one_field_raises_value_error():
    """acreate() with a reverse OneToOne field name raises ValueError (query.py line 258)."""
    from biz.models import Person

    with pytest.raises(ValueError, match="do not exist in this model"):
        await Person.objects.acreate(first_name="ReverseTest", employee="bad")


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_or_create_non_concrete_default_takes_full_save_path():
    """aupdate_or_create() with non-concrete field in defaults falls through to asave() without update_fields (query.py line 530)."""
    await Client.objects.acreate(name="UOC_NC", metadata={})
    obj, created = await Client.objects.aupdate_or_create(
        name="UOC_NC",
        defaults={"extra_attr": "some_value"},
    )
    assert not created
    assert obj.name == "UOC_NC"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_create_with_unique_and_update_fields():
    """abulk_create() with unique_fields/update_fields exercises field resolution at lines 323 and 328.
    The actual SQL fails (no DB constraint on name), but the field resolution lines still execute.
    """
    from django.db import ProgrammingError

    with pytest.raises(ProgrammingError):
        await Client.objects.abulk_create(
            [Client(name="BulkUF_A", metadata={"v": 1})],
            update_conflicts=True,
            unique_fields=["name"],
            update_fields=["metadata"],
        )


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acollect_keep_parents_skips_parent_relations():
    """acollect(keep_parents=True) skips parent relations (deletion.py lines 105->120, 128)."""
    from django.db.models.deletion import Collector
    from biz.models import Employee

    emp = await Employee.objects.acreate(first_name="KP_Test", department="QA")
    collector = Collector(using="default")
    await collector.acollect([emp], keep_parents=True)
    # With keep_parents=True, the Person parent should NOT be in the delete set
    from biz.models import Person
    assert Person not in collector.data


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_asave_with_positional_args_deprecated_path():
    """asave() with positional args uses the deprecated _parse_save_params path (models/__init__.py line 31)."""
    import warnings
    client = await Client.objects.acreate(name="PosArgs", metadata={})
    # Positional args: force_insert=False, force_update=False (RemovedInDjango60Warning)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        client.name = "PosArgsUpdated"
        await client.asave(False, False)
    refreshed = await Client.objects.aget(pk=client.pk)
    assert refreshed.name == "PosArgsUpdated"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_descending_annotation_covers_desc_branch():
    """aupdate() with descending non-aggregate annotation in order_by calls .desc() (query.py line 740)."""
    from django.db.models.functions import Length

    await Client.objects.acreate(name="DescAnno", metadata={})
    count = await (
        Client.objects.annotate(name_len=Length("name"))
        .order_by("-name_len")
        .filter(name="DescAnno")
        .aupdate(metadata={"desc": True})
    )
    assert count == 1


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_asave_on_deferred_queryset_triggers_loaded_fields_path():
    """asave() on a deferred model instance triggers update_fields from loaded fields (models/__init__.py lines 90-93)."""
    client = await Client.objects.acreate(name="DeferTest", metadata={"x": 1})
    # Fetch with metadata deferred — only name is loaded
    deferred_client = await Client.objects.defer("metadata").aget(pk=client.pk)
    deferred_client.name = "DeferTestUpdated"
    await deferred_client.asave()
    refreshed = await Client.objects.aget(pk=client.pk)
    assert refreshed.name == "DeferTestUpdated"
    # metadata should not have been overwritten (deferred field preserved)
    assert refreshed.metadata == {"x": 1}


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_non_aggregate_expression_raises_type_error():
    """aaggregate() with a non-aggregate expression raises TypeError (sql/__init__.py line 31)."""
    from django.db.models import Value

    with pytest.raises(TypeError, match="not an aggregate expression"):
        await Client.objects.aaggregate(my_val=Value(42))


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_reused_column_in_multiple_aggregates():
    """aaggregate() with two aggregates on the same column reuses col_refs (sql/__init__.py lines 140->146)."""
    from django.db.models import Sum, Avg

    client = await Client.objects.acreate(name="MultiAgg")
    await Invoice.objects.acreate(client=client, reference="MA-001", total=Decimal("10.00"))
    await Invoice.objects.acreate(client=client, reference="MA-002", total=Decimal("20.00"))
    result = await Invoice.objects.filter(client=client).aaggregate(
        total_sum=Sum("total"), total_avg=Avg("total")
    )
    assert result["total_sum"] == Decimal("30.00")
    assert result["total_avg"] == Decimal("15.00")


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_on_empty_queryset_returns_empty_result():
    """aaggregate() on impossible filter (EmptyResultSet) returns empty_set_result (sql/__init__.py line 198)."""
    from django.db.models import Sum

    result = await Invoice.objects.filter(id__in=[]).aaggregate(total=Sum("total"))
    assert result == {"total": None}


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_create_proxy_model_same_concrete_takes_false_branch():
    """abulk_create() on a proxy model: parent's concrete_model == self's → no raise (query.py line 316->315)."""
    from django.db import models

    class ProxyClient(Client):
        class Meta:
            proxy = True
            app_label = "biz"

    objs = await ProxyClient.objects.abulk_create(
        [ProxyClient(name="BulkProxy_A"), ProxyClient(name="BulkProxy_B")]
    )
    assert len(objs) == 2


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_delete_with_cross_table_filter_uses_subquery():
    """DELETE with a cross-table filter generates a subquery (SQLDeleteCompilerOverrides lines 583-595)."""
    client = await Client.objects.acreate(name="DeleteSubquery_Client")
    invoice = await Invoice.objects.acreate(
        client=client, reference="DS-001", total=Decimal("50.00")
    )
    # Filter Invoice by related Client name → JOIN → single_alias=False → subquery DELETE
    count = await Invoice.objects.filter(client__name="DeleteSubquery_Client")._araw_delete(using="default")
    assert count == 1
    assert not await Invoice.objects.filter(pk=invoice.pk).aexists()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiter_reverse_fk_populates_known_related_objects():
    """Iterating client.invoices.all() uses _known_related_objects (lines 173-181)."""
    client = await Client.objects.acreate(name="KnownRel_Client_Xq9")
    await Invoice.objects.acreate(
        client=client, reference="KR-001", total=Decimal("5.00")
    )
    # Reverse FK manager sets _known_related_objects = {client_field: {pk: client}}
    invoices = [inv async for inv in client.invoices.all()]
    assert len(invoices) == 1
    assert invoices[0].client_id == client.pk


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiter_select_related_and_known_objects_covers_is_cached():
    """select_related + reverse FK manager → is_cached(obj) True → line 174 continue."""
    client = await Client.objects.acreate(name="IsCache_Client_Za1")
    await Invoice.objects.acreate(
        client=client, reference="IC-001", total=Decimal("5.00")
    )
    # client.invoices sets _known_related_objects, select_related populates cache via populators
    invoices = [inv async for inv in client.invoices.select_related("client").all()]
    assert len(invoices) == 1
    assert invoices[0].client_id == client.pk


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_ain_bulk_returns_empty_dict_for_empty_list():
    """ain_bulk([]) with explicit empty id_list returns {} (query.py line 641-642)."""
    await Client.objects.acreate(name="InBulk_A")
    result = await Client.objects.ain_bulk([])
    assert result == {}


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_with_no_fields_returns_zero():
    """aupdate() with no fields generates empty SQL → EmptyResultSet path (compiler.py:301, 608)."""
    await Client.objects.acreate(name="EmptyUpdate_Xq7")
    count = await Client.objects.filter(name="EmptyUpdate_Xq7").aupdate()
    assert count == 0


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_sliced_queryset_raises_type_error():
    """_aupdate() on sliced queryset raises TypeError (query.py line 769)."""
    await Client.objects.acreate(name="SliceUpdate_Zz8")
    qs = Client.objects.all()[:5]
    with pytest.raises(TypeError, match="slice"):
        await qs._aupdate([])


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_or_create_with_auto_now_field_covers_update_fields_path():
    """aupdate_or_create on Client (has updated_at auto_now=True) → update_fields path lines 521-527."""
    # First call creates the object
    _, created = await Client.objects.aupdate_or_create(
        name="UOC_AutoNow_Zz9",
        defaults={"metadata": {"v": 1}},
    )
    assert created
    # Second call should update with auto_now field triggering custom pre_save path (line 525)
    client2, created2 = await Client.objects.aupdate_or_create(
        name="UOC_AutoNow_Zz9",
        defaults={"metadata": {"v": 2}},
    )
    assert not created2
    assert client2.metadata == {"v": 2}


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_with_existing_annotation_uses_subquery_with_group_by():
    """annotate(agg).aaggregate() → has_existing_aggregation=True → sql/__init__.py line 106."""
    from django.db.models import Max
    client = await Client.objects.acreate(name="ExistAgg_Client_Zz1")
    await Invoice.objects.acreate(client=client, reference="EA-001", total=Decimal("5.00"))
    result = await (
        Client.objects.annotate(inv_count=Count("invoices"))
        .aaggregate(max_invoices=Max("inv_count"))
    )
    assert result["max_invoices"] >= 0


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_with_values_annotate_covers_group_by_tuple_path():
    """values().annotate().aaggregate() → group_by is tuple → sql/__init__.py lines 117-118."""
    from django.db.models import Max
    client = await Client.objects.acreate(name="ValAgg_A_Zz2")
    await Invoice.objects.acreate(client=client, reference="VA-001", total=Decimal("5.00"))
    result = await Client.objects.filter(
        name__startswith="ValAgg_"
    ).values("name").annotate(inv_count=Count("invoices")).aaggregate(max_count=Max("inv_count"))
    assert result["max_count"] >= 0


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_annotation_with_group_by_cols_covers_line_125():
    """Non-aggregate annotation has group_by_cols → annotation_mask.add() at sql/__init__.py line 125."""
    from django.db.models import Max
    from django.db.models.functions import Length
    await Client.objects.acreate(name="GroupByCols_A_Zz3")
    result = await Client.objects.filter(
        name__startswith="GroupByCols_"
    ).values("name").annotate(
        name_len=Length("name"), inv_count=Count("invoices")
    ).aaggregate(max_len=Max("name_len"))
    assert result["max_len"] >= 0


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aaggregate_sliced_with_two_aggregates_on_same_column():
    """Sliced queryset (is_sliced=True) with two aggregates on same col → col_ref reuse at sql/__init__.py line 140→146."""
    from django.db.models import Sum, Avg
    client = await Client.objects.acreate(name="SlicedAgg_Zz4")
    await Invoice.objects.acreate(client=client, reference="SA-001", total=Decimal("10.00"))
    await Invoice.objects.acreate(client=client, reference="SA-002", total=Decimal("20.00"))
    result = await Invoice.objects.filter(client=client)[:5].aaggregate(
        total_sum=Sum("total"), total_avg=Avg("total")
    )
    assert result["total_sum"] == Decimal("30.00")


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_or_create_invoice_fk_attname_branch():
    """aupdate_or_create on Invoice: FK client field adds both field.name and attname to update_fields (query.py line 527)."""
    client = await Client.objects.acreate(name="UOC_FK_Zz5")
    await Invoice.objects.acreate(client=client, reference="UOC-FK-Zz5", total=Decimal("10.00"))

    # Update path: defaults={"total"} → update_fields={"total"} → loop hits client FK
    # client.name("client") != client.attname("client_id") → line 527 executes
    inv, created = await Invoice.objects.aupdate_or_create(
        reference="UOC-FK-Zz5",
        client=client,
        defaults={"total": Decimal("20.00")},
    )
    assert not created
    assert inv.total == Decimal("20.00")
