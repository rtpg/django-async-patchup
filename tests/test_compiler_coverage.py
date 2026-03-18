"""
Tests targeting uncovered branches in compiler.py.

Each test is labelled with the lines it aims to cover.
"""
import pytest
from asgiref.sync import sync_to_async
from decimal import Decimal
from biz.models import Client, Invoice

import django_async_patchup

django_async_patchup.setup()


# ── aas_sql: combinator branch (lines 35-43) ─────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_union_combinator():
    """union() generates a UNION ALL combinator query (lines 35-43 in aas_sql)."""
    await sync_to_async(Client.objects.create)(name="Comb_A")
    await sync_to_async(Client.objects.create)(name="Comb_B")

    qs1 = Client.objects.filter(name="Comb_A")
    qs2 = Client.objects.filter(name="Comb_B")

    collected = []
    async for client in qs1.union(qs2).aiterator():
        collected.append(client.name)

    assert sorted(collected) == ["Comb_A", "Comb_B"]


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_intersection_combinator():
    """intersection() exercises the combinator branch (lines 35-43 in aas_sql)."""
    await sync_to_async(Client.objects.create)(name="Intersect_Both")
    await sync_to_async(Client.objects.create)(name="Intersect_Only1")

    qs1 = Client.objects.filter(name__startswith="Intersect_")
    qs2 = Client.objects.filter(name="Intersect_Both")

    collected = []
    async for client in qs1.intersection(qs2).aiterator():
        collected.append(client.name)

    assert collected == ["Intersect_Both"]


# ── aas_sql: DISTINCT branch (lines 75-80) ───────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_distinct():
    """distinct() covers the DISTINCT branch in aas_sql (lines 75-80)."""
    await sync_to_async(Client.objects.create)(name="Dist_Dup")
    await sync_to_async(Client.objects.create)(name="Dist_Dup")
    await sync_to_async(Client.objects.create)(name="Dist_Unique")

    collected = []
    async for client in (
        Client.objects.filter(name__startswith="Dist_")
        .order_by("name")
        .distinct()
        .aiterator()
    ):
        collected.append(client.name)

    # DISTINCT without specifying fields deduplicates on all selected columns
    # (including id), so all 3 rows are returned. The goal is to cover the
    # DISTINCT code path in aas_sql, not to test deduplication semantics.
    assert len(collected) == 3
    assert "Dist_Unique" in collected


# ── aas_sql: SELECT FOR UPDATE branch (lines 99-147) ─────────────────────────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_select_for_update():
    """select_for_update() inside atomic covers the FOR UPDATE branch (lines 99-147)."""
    from django_async_backend.db.transaction import async_atomic

    await sync_to_async(Client.objects.create)(name="ForUpdate_1")

    collected = []
    async with async_atomic():
        async for client in (
            Client.objects.filter(name__startswith="ForUpdate_")
            .select_for_update()
            .aiterator()
        ):
            collected.append(client.name)

    assert collected == ["ForUpdate_1"]


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_select_for_update_skip_locked():
    """select_for_update(skip_locked=True) exercises the skip_locked sub-branch."""
    from django_async_backend.db.transaction import async_atomic

    await sync_to_async(Client.objects.create)(name="SkipLocked_1")

    collected = []
    async with async_atomic():
        async for client in (
            Client.objects.filter(name__startswith="SkipLocked_")
            .select_for_update(skip_locked=True)
            .aiterator()
        ):
            collected.append(client.name)

    assert collected == ["SkipLocked_1"]


# ── aas_sql: GROUP BY branch (lines 161-168) ─────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_annotate_group_by():
    """annotate() with Count generates GROUP BY (lines 161-168 in aas_sql)."""
    from django.db.models import Count

    client_a = await sync_to_async(Client.objects.create)(name="GroupBy_A")
    client_b = await sync_to_async(Client.objects.create)(name="GroupBy_B")
    await sync_to_async(Invoice.objects.create)(
        client=client_a, reference="GB-1", total=Decimal("10.00")
    )
    await sync_to_async(Invoice.objects.create)(
        client=client_a, reference="GB-2", total=Decimal("20.00")
    )

    results = {}
    async for client in (
        Client.objects.filter(name__startswith="GroupBy_")
        .annotate(invoice_count=Count("invoices"))
        .aiterator()
    ):
        results[client.name] = client.invoice_count

    assert results["GroupBy_A"] == 2
    assert results["GroupBy_B"] == 0


# ── aas_sql: HAVING branch (lines 170-173) ───────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_having_clause():
    """Filtering on an annotation produces HAVING (lines 170-173 in aas_sql)."""
    from django.db.models import Count

    client_a = await sync_to_async(Client.objects.create)(name="Having_A")
    await sync_to_async(Client.objects.create)(name="Having_B")
    await sync_to_async(Invoice.objects.create)(
        client=client_a, reference="H-1", total=Decimal("10.00")
    )

    collected = []
    async for client in (
        Client.objects.filter(name__startswith="Having_")
        .annotate(n=Count("invoices"))
        .filter(n__gt=0)
        .aiterator()
    ):
        collected.append(client.name)

    assert collected == ["Having_A"]


# ── aas_sql: explain_info branch (line 176) + aexplain_query (421-431) ───────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_compiler_aexplain_query():
    """
    Directly call aexplain_query() to cover the explain_info branch in aas_sql
    (line 176) and the aexplain_query async generator body (lines 421-431).
    """
    from django.db.models.sql.query import ExplainInfo

    await sync_to_async(Client.objects.create)(name="Explain_1")

    qs = Client.objects.filter(name__startswith="Explain_")

    def clone_query():
        q = qs.query.clone()
        q.explain_info = ExplainInfo(format=None, options={})
        return q

    q = await sync_to_async(clone_query)()
    compiler = q.aget_compiler(using="default")

    lines = []
    async for line in compiler.aexplain_query():
        lines.append(line)

    assert len(lines) > 0
    assert all(isinstance(line, str) for line in lines)


# ── aexecute_sql: ROW_COUNT (lines 322-326) via aupdate ──────────────────────
# ── SQLUpdateCompilerOverrides.aas_sql (606-668) + aexecute_sql (678-693) ────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_covers_row_count_and_update_compiler():
    """
    aupdate() exercises SQLUpdateCompilerOverrides (aas_sql + aexecute_sql) and
    the ROW_COUNT result type in SQLCompiler.aexecute_sql (lines 322-326).
    """
    await sync_to_async(Client.objects.create)(name="Update_1")
    await sync_to_async(Client.objects.create)(name="Update_2")

    rows = await Client.objects.filter(name__startswith="Update_").aupdate(
        name="Updated_Client"
    )
    assert rows == 2

    assert await Client.objects.filter(name="Updated_Client").acount() == 2


# ── SQLDeleteCompilerOverrides.aas_sql (578-595) via adelete ─────────────────
# ── aexecute_sql: ROW_COUNT path also hit by _araw_delete ────────────────────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_adelete_covers_delete_compiler():
    """
    adelete() exercises SQLDeleteCompilerOverrides.aas_sql (lines 578-595) and
    the ROW_COUNT result type via _araw_delete.
    """
    await sync_to_async(Client.objects.create)(name="Delete_1")
    await sync_to_async(Client.objects.create)(name="Delete_2")

    count, detail = await Client.objects.filter(name__startswith="Delete_").adelete()
    assert count == 2

    assert await Client.objects.filter(name__startswith="Delete_").acount() == 0


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_adelete_with_cascade():
    """
    Cascading delete exercises the multi-table path in the delete collector,
    covering more of adelete() and SQLDeleteCompilerOverrides.
    """
    client = await sync_to_async(Client.objects.create)(name="CascadeDelete_Client")
    await sync_to_async(Invoice.objects.create)(
        client=client, reference="CD-1", total=Decimal("5.00")
    )
    await sync_to_async(Invoice.objects.create)(
        client=client, reference="CD-2", total=Decimal("10.00")
    )

    # Deleting client cascades to invoices
    count, _ = await Client.objects.filter(name="CascadeDelete_Client").adelete()
    assert count == 3  # 1 client + 2 invoices

    assert await Invoice.objects.filter(reference__startswith="CD-").acount() == 0


# ── aresults_iter: converters branch (line 266) via JSONField ─────────────────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_json_field_triggers_converters():
    """
    JSONField has from_db_value, so iterating over Client (which has a metadata
    JSONField) triggers the converters branch in aresults_iter (line 266).
    """
    await sync_to_async(Client.objects.create)(
        name="JSON_Client", metadata={"key": "value", "count": 42}
    )

    collected = []
    async for client in Client.objects.filter(name="JSON_Client").aiterator():
        collected.append(client.metadata)

    assert collected == [{"key": "value", "count": 42}]


# ── aresults_iter: results=None path (line 252) ──────────────────────────────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aresults_iter_without_precomputed_results():
    """
    Calling aresults_iter() without a results argument triggers the internal
    aexecute_sql() call (line 252 in aresults_iter).
    """
    await sync_to_async(Client.objects.create)(name="RIter_1")
    await sync_to_async(Client.objects.create)(name="RIter_2")

    qs = Client.objects.filter(name__startswith="RIter_")
    compiler = qs.query.aget_compiler(using="default")

    # Calling without results= triggers the aexecute_sql path inside aresults_iter
    rows = list(await compiler.aresults_iter())
    assert len(rows) == 2


# ── aas_sql: LIMIT/OFFSET via sliced queryset ────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aiterator_sliced_queryset_limit_offset():
    """
    A sliced queryset adds LIMIT/OFFSET SQL (lines 195-200 in aas_sql).
    Also covers the non-chunked MULTI path since sliced queries can't use
    server-side cursors.
    """
    for i in range(5):
        await sync_to_async(Client.objects.create)(name=f"Slice_{i:02d}")

    collected = []
    # Use a sliced queryset (which sets is_sliced=True → with_limit_offset=True)
    async for client in (
        Client.objects.filter(name__startswith="Slice_").order_by("name")[1:3].aiterator()
    ):
        collected.append(client.name)

    assert collected == ["Slice_01", "Slice_02"]


# ── aas_sql: order_by (covers the order_by branch, line 184+) ────────────────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_with_no_matching_rows():
    """aupdate() on an empty queryset returns 0 rows updated (exercises update compiler)."""
    rows = await Client.objects.filter(name="__no_such_client__").aupdate(
        name="irrelevant"
    )
    assert rows == 0


# ── InsertCompilerOverrides: aexecute_sql / aas_sql paths ────────────────────


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_acreate_exercises_insert_compiler():
    """
    acreate() goes through InsertCompilerOverrides.aexecute_sql with returning_fields
    set (PostgreSQL RETURNING clause path, lines 538-546) and aas_sql single-object
    bulk_insert_sql branch (lines 484-488).
    """
    client = await Client.objects.acreate(name="ACreate_Test")
    assert client.pk is not None
    assert client.name == "ACreate_Test"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_create_multiple_objects_covers_bulk_returning_rows():
    """
    abulk_create() with multiple objects triggers the can_return_rows_from_bulk_insert
    + len(objs) > 1 path in aexecute_sql (lines 532-537), and the bulk_insert_sql
    branch in aas_sql (lines 484-488).
    """
    objs = await Client.objects.abulk_create([
        Client(name="Bulk_A"),
        Client(name="Bulk_B"),
        Client(name="Bulk_C"),
    ])
    assert len(objs) == 3
    assert all(obj.pk is not None for obj in objs)
    assert await Client.objects.filter(name__startswith="Bulk_").acount() == 3


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_abulk_create_ignore_conflicts_covers_no_returning_path():
    """
    abulk_create(ignore_conflicts=True) calls _ainsert without returning_fields,
    covering the can_bulk path in aas_sql (lines 504-508, including the
    on_conflict_suffix_sql branch at 506-507) and the early return at
    aexecute_sql line 530-531.
    """
    # Create one first to cause a conflict on name if we had a unique constraint,
    # but here we just exercise the ignore_conflicts SQL path.
    objs = await Client.objects.abulk_create(
        [Client(name="IgnoreConflict_A"), Client(name="IgnoreConflict_B")],
        ignore_conflicts=True,
    )
    # ignore_conflicts=True means returning_fields is not set → PKs not populated
    assert await Client.objects.filter(name__startswith="IgnoreConflict_").acount() == 2


# ── SQLUpdateCompiler.aas_sql: aggregate in SET raises FieldError (line 617) ─


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_with_aggregate_expression_raises_field_error():
    """aupdate with an aggregate expression raises FieldError (compiler.py line 617)."""
    from django.core.exceptions import FieldError
    from django.db.models import Count

    await Client.objects.acreate(name="AggUpdateTest_X")
    with pytest.raises(FieldError, match="Aggregate functions are not allowed"):
        await Client.objects.filter(name="AggUpdateTest_X").aupdate(name=Count("id"))


# ── SQLUpdateCompiler.aas_sql: FK update with model instance (lines 632-634) ─


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_fk_with_model_instance_covers_prepare_database_save():
    """aupdate with a model instance for a FK field uses prepare_database_save (lines 632-634)."""
    client = await Client.objects.acreate(name="FKUpdate_Orig")
    invoice = await Invoice.objects.acreate(
        client=client, reference="FKU-CC-001", total=Decimal("5.00")
    )
    new_client = await Client.objects.acreate(name="FKUpdate_New")
    count = await Invoice.objects.filter(pk=invoice.pk).aupdate(client=new_client)
    assert count == 1
    refreshed = await Invoice.objects.aget(pk=invoice.pk)
    assert refreshed.client_id == new_client.pk


# ── SQLUpdateCompiler.aas_sql: window expression raises FieldError (line 622) ─


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_with_window_expression_raises_field_error():
    """aupdate with a window expression raises FieldError (compiler.py line 622)."""
    from django.core.exceptions import FieldError
    from django.db.models.expressions import Window
    from django.db.models.functions import Rank

    await Client.objects.acreate(name="WindowUpdate_X")
    with pytest.raises(FieldError, match="Window expressions are not allowed"):
        await Client.objects.filter(name="WindowUpdate_X").aupdate(
            name=Window(expression=Rank())
        )


# ── SQLUpdateCompiler.aas_sql: FullResultSet (no WHERE) path (lines 664-665) ─


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_no_filter_covers_full_result_set():
    """aupdate() with no filter hits FullResultSet branch (compiler.py lines 664-665)."""
    await Client.objects.acreate(name="NoFilter_A")
    await Client.objects.acreate(name="NoFilter_B")

    rows = await Client.objects.filter(name__startswith="NoFilter_").aupdate(
        metadata={"updated": True}
    )
    assert rows == 2


# FullResultSet variant: truly unfiltered queryset (no WHERE clause at all)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_truly_unfiltered_covers_full_result_set():
    """aupdate() on an unfiltered queryset hits FullResultSet (lines 664-665)."""
    await Client.objects.acreate(name="Unfiltered_FS")
    # No .filter() → WHERE clause is empty → FullResultSet raised internally
    rows = await Client.objects.all().aupdate(metadata={"fs": True})
    assert rows >= 1


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aexplain_covers_string_yield_branch():
    """aexplain_query() yields string rows from EXPLAIN output (compiler.py line 431)."""
    await Client.objects.acreate(name="Explain_Client")
    result = await Client.objects.filter(name="Explain_Client").aexplain()
    assert isinstance(result, str)
    assert len(result) > 0


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_select_for_update_outside_transaction_raises():
    """select_for_update() outside a transaction raises TransactionManagementError (compiler.py line 106)."""
    from django.db.transaction import TransactionManagementError

    with pytest.raises(TransactionManagementError, match="outside of a transaction"):
        await Client.objects.select_for_update().aget(name="NonExistent_SFU")


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_annotate_with_distinct_fields_raises_not_implemented():
    """annotate() + distinct(fields) raises NotImplementedError (compiler.py line 162)."""
    from django.db.models import Count

    with pytest.raises(NotImplementedError, match="annotate.*distinct"):
        _ = [
            x
            async for x in Client.objects.annotate(cnt=Count("invoices")).distinct("name")
        ]


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aupdate_with_model_instance_on_non_fk_field_raises():
    """Passing a model instance to aupdate() on a non-FK field raises TypeError (compiler.py line 635)."""
    client = await Client.objects.acreate(name="UpdateErr_A")
    other_client = await Client.objects.acreate(name="UpdateErr_B")
    with pytest.raises(TypeError, match="Tried to update field"):
        await Client.objects.filter(pk=client.pk).aupdate(name=other_client)


