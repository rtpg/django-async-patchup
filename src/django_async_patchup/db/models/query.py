#!/usr/bin/env python3

from django.db.models.query import *
from django.db.models import Manager
from django_async_patchup.registry import from_codegen, generate_unasynced, just_patch
from django_async_patchup import ASYNC_TRUTH_MARKER
from django_async_patchup.db import should_use_sync_fallback
from django.db.models.sql.constants import CURSOR

from django_async_backend.db import async_connections
from django_async_backend.db.transaction import async_atomic, async_mark_for_rollback_on_error


class QuerySetOverrides:

    @generate_unasynced(sync_variant=QuerySet._fetch_all)
    async def _afetch_all(self):
        if self._result_cache is None:
            if ASYNC_TRUTH_MARKER:
                self._result_cache = [elt async for elt in self._iterable_class(self)]
            else:
                self._result_cache = list(self._iterable_class(self))
        if self._prefetch_related_lookups and not self._prefetch_done:
            await self._aprefetch_related_objects()

    @just_patch(onto=(QuerySet, "__aiter__"))
    def __aiter__(self):
        async def generator():
            await self._afetch_all()
            for item in self._result_cache:
                yield item

        return generator()

    @staticmethod
    def _fetch_then_len(qs):
        qs._fetch_all()
        return len(qs._result_cache)

    @just_patch(onto=QuerySet)
    async def _afetch_then_len(self):
        # TODO link to _fetch_then_len
        # RawQuerySet helper
        await self._afetch_all()
        return len(self._result_cache)

    @generate_unasynced(sync_variant=QuerySet.aggregate)
    async def aaggregate(self, *args, **kwargs):
        """
        Return a dictionary containing the calculations (aggregation)
        over the current queryset.

        If args is present the expression is passed as a kwarg using
        the Aggregate object's default alias.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.aggregate)(*args, **kwargs)
        if self.query.distinct_fields:
            raise NotImplementedError("aggregate() + distinct(fields) not implemented.")
        self._validate_values_are_expressions(
            (*args, *kwargs.values()), method_name="aggregate"
        )
        for arg in args:
            # The default_alias property raises TypeError if default_alias
            # can't be set automatically or AttributeError if it isn't an
            # attribute.
            try:
                arg.default_alias
            except (AttributeError, TypeError):
                raise TypeError("Complex aggregates require an alias")
            kwargs[arg.default_alias] = arg

        return await self.query.chain().aget_aggregation(self.db, kwargs)


@just_patch(onto=Manager)
async def _ainsert(
    self,
    objs,
    fields,
    returning_fields=None,
    raw=False,
    using=None,
    on_conflict=None,
    update_fields=None,
    unique_fields=None,
):
    """
    Async version of Manager._insert.
    Insert a new record for the given model.
    """
    self._for_write = True
    if using is None:
        using = self.db
    query = sql.InsertQuery(
        self.model,
        on_conflict=on_conflict,
        update_fields=update_fields,
        unique_fields=unique_fields,
    )
    query.insert_values(fields, objs, raw=raw)
    return await query.aget_compiler(using=using).aexecute_sql(returning_fields)


class ModelIterableOverrides:

    @just_patch(onto=(ModelIterable, "__aiter__"))
    def __aiter__(self):
        if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
            return self._async_generator()
        else:
            print("USING AGENERATOR")
            return self.a__iter__()

    # Yes the name for the following is silly, this gives
    # me the right kinda naming needs for codegen
    @generate_unasynced(sync_variant=ModelIterable.__iter__)
    async def a__iter__(self):
        queryset = self.queryset
        db = queryset.db
        if ASYNC_TRUTH_MARKER:
            compiler = queryset.query.aget_compiler(using=db)
        else:
            compiler = queryset.query.get_compiler(using=db)
        # Execute the query. This will also fill compiler.select, klass_info,
        # and annotations.
        results = await compiler.aexecute_sql(
            chunked_fetch=self.chunked_fetch, chunk_size=self.chunk_size
        )
        select, klass_info, annotation_col_map = (
            compiler.select,
            compiler.klass_info,
            compiler.annotation_col_map,
        )
        model_cls = klass_info["model"]
        select_fields = klass_info["select_fields"]
        model_fields_start, model_fields_end = select_fields[0], select_fields[-1] + 1
        init_list = [
            f[0].target.attname for f in select[model_fields_start:model_fields_end]
        ]
        related_populators = get_related_populators(klass_info, select, db)
        known_related_objects = [
            (
                field,
                related_objs,
                operator.attrgetter(
                    *[
                        (
                            field.attname
                            if from_field == "self"
                            else queryset.model._meta.get_field(from_field).attname
                        )
                        for from_field in field.from_fields
                    ]
                ),
            )
            for field, related_objs in queryset._known_related_objects.items()
        ]
        for row in await compiler.aresults_iter(results):
            obj = model_cls.from_db(
                db, init_list, row[model_fields_start:model_fields_end]
            )
            for rel_populator in related_populators:
                rel_populator.populate(row, obj)
            if annotation_col_map:
                for attr_name, col_pos in annotation_col_map.items():
                    setattr(obj, attr_name, row[col_pos])

            # Add the known related objects to the model.
            for field, rel_objs, rel_getter in known_related_objects:
                # Avoid overwriting objects loaded by, e.g., select_related().
                if field.is_cached(obj):
                    continue
                rel_obj_id = rel_getter(obj)
                try:
                    rel_obj = rel_objs[rel_obj_id]
                except KeyError:
                    pass  # May happen in qs1 | qs2 scenarios.
                else:
                    setattr(obj, field.name, rel_obj)

            yield obj

    @generate_unasynced(sync_variant=QuerySet.count)
    async def acount(self):
        """
        Perform a SELECT COUNT() and return the number of records as an
        integer.

        If the QuerySet is already fully cached, return the length of the
        cached results set to avoid multiple SELECT COUNT(*) calls.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.count)()
        if self._result_cache is not None:
            return len(self._result_cache)

        return await self.query.aget_count(using=self.db)

    @generate_unasynced(sync_variant=QuerySet.get)
    async def aget(self, *args, **kwargs):
        """
        Perform the query and return a single object matching the given
        keyword arguments.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.get)(*args, **kwargs)

        if self.query.combinator and (args or kwargs):
            raise NotSupportedError(
                "Calling QuerySet.get(...) with filters after %s() is not "
                "supported." % self.query.combinator
            )
        clone = self._chain() if self.query.combinator else self.filter(*args, **kwargs)
        if self.query.can_filter() and not self.query.distinct_fields:
            clone = clone.order_by()
        limit = None
        if (
            not clone.query.select_for_update
            or connections[clone.db].features.supports_select_for_update_with_limit
        ):
            limit = MAX_GET_RESULTS
            clone.query.set_limits(high=limit)
        if ASYNC_TRUTH_MARKER:
            num = await clone._afetch_then_len()
        else:
            num = len(clone)
        if num == 1:
            return clone._result_cache[0]
        if not num:
            raise self.model.DoesNotExist(
                "%s matching query does not exist." % self.model._meta.object_name
            )
        raise self.model.MultipleObjectsReturned(
            "get() returned more than one %s -- it returned %s!"
            % (
                self.model._meta.object_name,
                num if not limit or num < limit else "more than %s" % (limit - 1),
            )
        )

    @generate_unasynced(sync_variant=QuerySet.create)
    async def acreate(self, **kwargs):
        """
        Create a new object with the given kwargs, saving it to the database
        and returning the created object.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.create)(**kwargs)
        reverse_one_to_one_fields = frozenset(kwargs).intersection(
            self.model._meta._reverse_one_to_one_field_names
        )
        if reverse_one_to_one_fields:
            raise ValueError(
                "The following fields do not exist in this model: %s"
                % ", ".join(reverse_one_to_one_fields)
            )

        obj = self.model(**kwargs)
        self._for_write = True
        await obj.asave(force_insert=True, using=self.db)
        return obj

    acreate.alters_data = True

    @generate_unasynced(sync_variant=QuerySet.bulk_create)
    async def abulk_create(
        self,
        objs,
        batch_size=None,
        ignore_conflicts=False,
        update_conflicts=False,
        update_fields=None,
        unique_fields=None,
    ):
        """
        Insert each of the instances into the database. Do *not* call
        save() on each of the instances, do not send any pre/post_save
        signals, and do not set the primary key attribute if it is an
        autoincrement field (except if features.can_return_rows_from_bulk_insert=True).
        Multi-table models are not supported.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.bulk_create)(
                    objs=objs,
                    batch_size=batch_size,
                    ignore_conflicts=ignore_conflicts,
                    update_conflicts=update_conflicts,
                    update_fields=update_fields,
                    unique_fields=unique_fields,
                )
        # When you bulk insert you don't get the primary keys back (if it's an
        # autoincrement, except if can_return_rows_from_bulk_insert=True), so
        # you can't insert into the child tables which references this. There
        # are two workarounds:
        # 1) This could be implemented if you didn't have an autoincrement pk
        # 2) You could do it by doing O(n) normal inserts into the parent
        #    tables to get the primary keys back and then doing a single bulk
        #    insert into the childmost table.
        # We currently set the primary keys on the objects when using
        # PostgreSQL via the RETURNING ID clause. It should be possible for
        # Oracle as well, but the semantics for extracting the primary keys is
        # trickier so it's not done yet.
        if batch_size is not None and batch_size <= 0:
            raise ValueError("Batch size must be a positive integer.")
        # Check that the parents share the same concrete model with the our
        # model to detect the inheritance pattern ConcreteGrandParent ->
        # MultiTableParent -> ProxyChild. Simply checking self.model._meta.proxy
        # would not identify that case as involving multiple tables.
        for parent in self.model._meta.all_parents:
            if parent._meta.concrete_model is not self.model._meta.concrete_model:
                raise ValueError("Can't bulk create a multi-table inherited model")
        if not objs:
            return objs
        opts = self.model._meta
        if unique_fields:
            # Primary key is allowed in unique_fields.
            unique_fields = [
                self.model._meta.get_field(opts.pk.name if name == "pk" else name)
                for name in unique_fields
            ]
        if update_fields:
            update_fields = [self.model._meta.get_field(name) for name in update_fields]
        on_conflict = self._check_bulk_create_options(
            ignore_conflicts,
            update_conflicts,
            update_fields,
            unique_fields,
        )
        self._for_write = True
        fields = [f for f in opts.concrete_fields if not f.generated]
        objs = list(objs)
        self._prepare_for_bulk_create(objs)
        async with async_atomic(using=self.db, savepoint=False):
            objs_without_pk, objs_with_pk = partition(lambda o: o._is_pk_set(), objs)
            if objs_with_pk:
                returned_columns = await self._abatched_insert(
                    objs_with_pk,
                    fields,
                    batch_size,
                    on_conflict=on_conflict,
                    update_fields=update_fields,
                    unique_fields=unique_fields,
                )
                for obj_with_pk, results in zip(objs_with_pk, returned_columns):
                    for result, field in zip(results, opts.db_returning_fields):
                        if field != opts.pk:
                            setattr(obj_with_pk, field.attname, result)
                for obj_with_pk in objs_with_pk:
                    obj_with_pk._state.adding = False
                    obj_with_pk._state.db = self.db
            if objs_without_pk:
                fields = [f for f in fields if not isinstance(f, AutoField)]
                returned_columns = await self._abatched_insert(
                    objs_without_pk,
                    fields,
                    batch_size,
                    on_conflict=on_conflict,
                    update_fields=update_fields,
                    unique_fields=unique_fields,
                )
                connection = connections[self.db]
                if (
                    connection.features.can_return_rows_from_bulk_insert
                    and on_conflict is None
                ):
                    assert len(returned_columns) == len(objs_without_pk)
                for obj_without_pk, results in zip(objs_without_pk, returned_columns):
                    for result, field in zip(results, opts.db_returning_fields):
                        setattr(obj_without_pk, field.attname, result)
                    obj_without_pk._state.adding = False
                    obj_without_pk._state.db = self.db

        return objs

    abulk_create.alters_data = True

    @generate_unasynced(sync_variant=QuerySet.bulk_update)
    async def abulk_update(self, objs, fields, batch_size=None):
        """
        Update the given fields in each of the given objects in the database.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.bulk_update)(
                    objs=objs,
                    fields=fields,
                    batch_size=batch_size,
                )
        if batch_size is not None and batch_size <= 0:
            raise ValueError("Batch size must be a positive integer.")
        if not fields:
            raise ValueError("Field names must be given to bulk_update().")
        objs = tuple(objs)
        if not all(obj._is_pk_set() for obj in objs):
            raise ValueError("All bulk_update() objects must have a primary key set.")
        opts = self.model._meta
        fields = [opts.get_field(name) for name in fields]
        if any(not f.concrete or f.many_to_many for f in fields):
            raise ValueError("bulk_update() can only be used with concrete fields.")
        all_pk_fields = set(opts.pk_fields)
        for parent in opts.all_parents:
            all_pk_fields.update(parent._meta.pk_fields)
        if any(f in all_pk_fields for f in fields):
            raise ValueError("bulk_update() cannot be used with primary key fields.")
        if not objs:
            return 0
        for obj in objs:
            obj._prepare_related_fields_for_save(
                operation_name="bulk_update", fields=fields
            )
        # PK is used twice in the resulting update query, once in the filter
        # and once in the WHEN. Each field will also have one CAST.
        self._for_write = True
        connection = connections[self.db]
        max_batch_size = connection.ops.bulk_batch_size([opts.pk, opts.pk] + fields, objs)
        batch_size = min(batch_size, max_batch_size) if batch_size else max_batch_size
        requires_casting = connection.features.requires_casted_case_in_updates
        batches = (objs[i : i + batch_size] for i in range(0, len(objs), batch_size))
        updates = []
        for batch_objs in batches:
            update_kwargs = {}
            for field in fields:
                when_statements = []
                for obj in batch_objs:
                    attr = getattr(obj, field.attname)
                    if not hasattr(attr, "resolve_expression"):
                        attr = Value(attr, output_field=field)
                    when_statements.append(When(pk=obj.pk, then=attr))
                case_statement = Case(*when_statements, output_field=field)
                if requires_casting:
                    case_statement = Cast(case_statement, output_field=field)
                update_kwargs[field.attname] = case_statement
            updates.append(([obj.pk for obj in batch_objs], update_kwargs))
        rows_updated = 0
        queryset = self.using(self.db)
        async with async_atomic(using=self.db, savepoint=False):
            for pks, update_kwargs in updates:
                rows_updated += await queryset.filter(pk__in=pks).aupdate(
                    **update_kwargs
                )
        return rows_updated

    abulk_update.alters_data = True

    @generate_unasynced(sync_variant=QuerySet.get_or_create)
    async def aget_or_create(self, defaults=None, **kwargs):
        """
        Look up an object with the given kwargs, creating one if necessary.
        Return a tuple of (object, created), where created is a boolean
        specifying whether an object was created.
        """
        # The get() needs to be targeted at the write database in order
        # to avoid potential transaction consistency problems.
        self._for_write = True
        try:
            return (await self.aget(**kwargs)), False
        except self.model.DoesNotExist:
            params = self._extract_model_params(defaults, **kwargs)
            # Try to create an object using passed params.
            try:
                async with async_atomic(using=self.db):
                    params = dict(resolve_callables(params))
                    return (await self.acreate(**params)), True
            except IntegrityError:
                try:
                    return (await self.aget(**kwargs)), False
                except self.model.DoesNotExist:
                    pass
                raise

    aget_or_create.alters_data = True

    @generate_unasynced(sync_variant=QuerySet.update_or_create)
    async def aupdate_or_create(self, defaults=None, create_defaults=None, **kwargs):
        """
        Look up an object with the given kwargs, updating one with defaults
        if it exists, otherwise create a new one. Optionally, an object can
        be created with different values than defaults by using
        create_defaults.
        Return a tuple (object, created), where created is a boolean
        specifying whether an object was created.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.update_or_create)(
                    defaults=defaults,
                    create_defaults=create_defaults,
                    **kwargs,
                )
        update_defaults = defaults or {}
        if create_defaults is None:
            create_defaults = update_defaults

        self._for_write = True
        async with async_atomic(using=self.db):
            # Lock the row so that a concurrent update is blocked until
            # update_or_create() has performed its save.
            obj, created = await self.select_for_update().aget_or_create(
                create_defaults, **kwargs
            )
            if created:
                return obj, created
            for k, v in resolve_callables(update_defaults):
                setattr(obj, k, v)

            update_fields = set(update_defaults)
            concrete_field_names = self.model._meta._non_pk_concrete_field_names
            # update_fields does not support non-concrete fields.
            if concrete_field_names.issuperset(update_fields):
                # Add fields which are set on pre_save(), e.g. auto_now fields.
                # This is to maintain backward compatibility as these fields
                # are not updated unless explicitly specified in the
                # update_fields list.
                pk_fields = self.model._meta.pk_fields
                for field in self.model._meta.local_concrete_fields:
                    if not (
                        field in pk_fields or field.__class__.pre_save is Field.pre_save
                    ):
                        update_fields.add(field.name)
                        if field.name != field.attname:
                            update_fields.add(field.attname)
                await obj.asave(using=self.db, update_fields=update_fields)
            else:
                await obj.asave(using=self.db)
        return obj, False

    aupdate_or_create.alters_data = True

    @generate_unasynced(sync_variant=QuerySet._earliest)
    async def _aearliest(self, *fields):
        """
        Return the earliest object according to fields (if given) or by the
        model's Meta.get_latest_by.
        """
        if fields:
            order_by = fields
        else:
            order_by = getattr(self.model._meta, "get_latest_by")
            if order_by and not isinstance(order_by, (tuple, list)):
                order_by = (order_by,)
        if order_by is None:
            raise ValueError(
                "earliest() and latest() require either fields as positional "
                "arguments or 'get_latest_by' in the model's Meta."
            )
        obj = self._chain()
        obj.query.set_limits(high=1)
        obj.query.clear_ordering(force=True)
        obj.query.add_ordering(*order_by)
        return await obj.aget()

    @generate_unasynced(sync_variant=QuerySet.earliest)
    async def aearliest(self, *fields):
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.earliest)(*fields)
        if self.query.is_sliced:
            raise TypeError("Cannot change a query once a slice has been taken.")
        return await self._aearliest(*fields)

    @generate_unasynced(sync_variant=QuerySet.latest)
    async def alatest(self, *fields):
        """
        Return the latest object according to fields (if given) or by the
        model's Meta.get_latest_by.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.latest)(*fields)
        if self.query.is_sliced:
            raise TypeError("Cannot change a query once a slice has been taken.")
        return await self.reverse()._aearliest(*fields)

    @generate_unasynced(sync_variant=QuerySet.first)
    async def afirst(self):
        """Return the first object of a query or None if no match is found."""
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.first)()
        if self.ordered:
            queryset = self
        else:
            self._check_ordering_first_last_queryset_aggregation(method="first")
            queryset = self.order_by("pk")
        async for obj in queryset[:1]:
            return obj

    @generate_unasynced(sync_variant=QuerySet.last)
    async def alast(self):
        """Return the last object of a query or None if no match is found."""
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.last)()
        if self.ordered:
            queryset = self.reverse()
        else:
            self._check_ordering_first_last_queryset_aggregation(method="last")
            queryset = self.order_by("-pk")
        async for obj in queryset[:1]:
            return obj

    @generate_unasynced(sync_variant=QuerySet.in_bulk)
    async def ain_bulk(self, id_list=None, *, field_name="pk"):
        """
        Return a dictionary mapping each of the given IDs to the object with
        that ID. If `id_list` isn't provided, evaluate the entire QuerySet.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.in_bulk)(
                    id_list=id_list,
                    field_name=field_name,
                )
        if self.query.is_sliced:
            raise TypeError("Cannot use 'limit' or 'offset' with in_bulk().")
        if not issubclass(self._iterable_class, ModelIterable):
            raise TypeError("in_bulk() cannot be used with values() or values_list().")
        opts = self.model._meta
        unique_fields = [
            constraint.fields[0]
            for constraint in opts.total_unique_constraints
            if len(constraint.fields) == 1
        ]
        if (
            field_name != "pk"
            and not opts.get_field(field_name).unique
            and field_name not in unique_fields
            and self.query.distinct_fields != (field_name,)
        ):
            raise ValueError(
                "in_bulk()'s field_name must be a unique field but %r isn't."
                % field_name
            )
        if id_list is not None:
            if not id_list:
                return {}
            filter_key = "{}__in".format(field_name)
            max_params = connections[self.db].features.max_query_params or 0
            num_fields = len(opts.pk_fields) if field_name == "pk" else 1
            batch_size = max_params // num_fields
            id_list = tuple(id_list)
            # If the database has a limit on the number of query parameters
            # (e.g. SQLite), retrieve objects in batches if necessary.
            if batch_size and batch_size < len(id_list):
                qs = ()
                for offset in range(0, len(id_list), batch_size):
                    batch = id_list[offset : offset + batch_size]
                    qs += tuple(self.filter(**{filter_key: batch}))
            else:
                qs = self.filter(**{filter_key: id_list})
        else:
            qs = self._chain()
        return {getattr(obj, field_name): obj async for obj in qs}

    @generate_unasynced(sync_variant=QuerySet.delete)
    async def adelete(self):
        """Delete the records in the current QuerySet."""
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.delete)()
        self._not_support_combined_queries("delete")
        if self.query.is_sliced:
            raise TypeError("Cannot use 'limit' or 'offset' with delete().")
        if self.query.distinct_fields:
            raise TypeError("Cannot call delete() after .distinct(*fields).")
        if self._fields is not None:
            raise TypeError("Cannot call delete() after .values() or .values_list()")

        del_query = self._chain()

        # The delete is actually 2 queries - one to find related objects,
        # and one to delete. Make sure that the discovery of related
        # objects is performed on the same database as the deletion.
        del_query._for_write = True

        # Disable non-supported fields.
        del_query.query.select_for_update = False
        del_query.query.select_related = False
        del_query.query.clear_ordering(force=True)

        collector = Collector(using=del_query.db, origin=self)
        await collector.acollect(del_query)
        num_deleted, num_deleted_per_model = await collector.adelete()

        # Clear the result cache, in case this QuerySet gets reused.
        self._result_cache = None
        return num_deleted, num_deleted_per_model

    adelete.alters_data = True
    adelete.queryset_only = True

    @generate_unasynced(sync_variant=QuerySet._raw_delete)
    async def _araw_delete(self, using):
        """
        Delete objects found from the given queryset in single direct SQL
        query. No signals are sent and there is no protection for cascades.
        """
        query = self.query.clone()
        query.__class__ = sql.DeleteQuery
        return await query.aget_compiler(using).aexecute_sql(ROW_COUNT)

    _araw_delete.alters_data = True

    @generate_unasynced(sync_variant=QuerySet.update)
    async def aupdate(self, **kwargs):
        """
        Update all elements in the current QuerySet, setting all the given
        fields to the appropriate values.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.update)(**kwargs)
        self._not_support_combined_queries("update")
        if self.query.is_sliced:
            raise TypeError("Cannot update a query once a slice has been taken.")
        self._for_write = True
        query = self.query.chain(sql.UpdateQuery)
        query.add_update_values(kwargs)

        # Inline annotations in order_by(), if possible.
        new_order_by = []
        for col in query.order_by:
            alias = col
            descending = False
            if isinstance(alias, str) and alias.startswith("-"):
                alias = alias.removeprefix("-")
                descending = True
            if annotation := query.annotations.get(alias):
                if getattr(annotation, "contains_aggregate", False):
                    raise exceptions.FieldError(
                        f"Cannot update when ordering by an aggregate: {annotation}"
                    )
                if descending:
                    annotation = annotation.desc()
                new_order_by.append(annotation)
            else:
                new_order_by.append(col)
        query.order_by = tuple(new_order_by)
        # Clear SELECT clause as all annotation references were inlined by
        # add_update_values() already.
        query.clear_select_clause()
        if ASYNC_TRUTH_MARKER:
            # XXX should fix codegen to handle this case
            async with async_mark_for_rollback_on_error(using=self.db):
                rows = await query.aget_compiler(self.db).aexecute_sql(ROW_COUNT)
        else:
            with transaction.mark_for_rollback_on_error(using=self.db):
                rows = query.get_compiler(self.db).execute_sql(ROW_COUNT)
        self._result_cache = None
        return rows

    aupdate.alters_data = True

    @generate_unasynced(sync_variant=QuerySet._update)
    async def _aupdate(self, values):
        """
        A version of update() that accepts field objects instead of field names.
        Used primarily for model saving and not intended for use by general
        code (it requires too much poking around at model internals to be
        useful at that level).
        """
        if self.query.is_sliced:
            raise TypeError("Cannot update a query once a slice has been taken.")
        query = self.query.chain(sql.UpdateQuery)
        query.add_update_fields(values)
        # Clear any annotations so that they won't be present in subqueries.
        query.annotations = {}
        self._result_cache = None
        return await query.aget_compiler(self.db).aexecute_sql(ROW_COUNT)

    _aupdate.alters_data = True
    _aupdate.queryset_only = False

    @generate_unasynced(sync_variant=QuerySet.exists)
    async def aexists(self):
        """
        Return True if the QuerySet would have any results, False otherwise.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.exists)()
        if self._result_cache is None:
            return await self.query.ahas_results(using=self.db)
        return bool(self._result_cache)

    @generate_unasynced(sync_variant=QuerySet.contains)
    async def acontains(self, obj):
        """
        Return True if the QuerySet contains the provided obj,
        False otherwise.
        """
        if ASYNC_TRUTH_MARKER:
            if should_use_sync_fallback(ASYNC_TRUTH_MARKER):
                return await sync_to_async(self.contains)(obj=obj)
        self._not_support_combined_queries("contains")
        if self._fields is not None:
            raise TypeError(
                "Cannot call QuerySet.contains() after .values() or .values_list()."
            )
        try:
            if obj._meta.concrete_model != self.model._meta.concrete_model:
                return False
        except AttributeError:
            raise TypeError("'obj' must be a model instance.")
        if not obj._is_pk_set():
            raise ValueError("QuerySet.contains() cannot be used on unsaved objects.")
        if self._result_cache is not None:
            return obj in self._result_cache
        return await self.filter(pk=obj.pk).aexists()

    @generate_unasynced(sync_variant=QuerySet._prefetch_related_objects)
    async def _aprefetch_related_objects(self):
        # This method can only be called once the result cache has been filled.
        await aprefetch_related_objects(self._result_cache, *self._prefetch_related_lookups)
        self._prefetch_done = True

    ###################
    # PRIVATE METHODS #
    ###################

    ###################
    # PRIVATE METHODS #
    ###################

    @generate_unasynced(sync_variant=QuerySet._insert)
    async def _ainsert(
        self,
        objs,
        fields,
        returning_fields=None,
        raw=False,
        using=None,
        on_conflict=None,
        update_fields=None,
        unique_fields=None,
    ):
        """
        Insert a new record for the given model. This provides an interface to
        the InsertQuery class and is how Model.save() is implemented.
        """
        self._for_write = True
        if using is None:
            using = self.db
        query = sql.InsertQuery(
            self.model,
            on_conflict=on_conflict,
            update_fields=update_fields,
            unique_fields=unique_fields,
        )
        query.insert_values(fields, objs, raw=raw)
        return await query.aget_compiler(using=using).aexecute_sql(returning_fields)

    _ainsert.alters_data = True
    _ainsert.queryset_only = False

    @generate_unasynced(sync_variant=QuerySet._batched_insert)
    async def _abatched_insert(
        self,
        objs,
        fields,
        batch_size,
        on_conflict=None,
        update_fields=None,
        unique_fields=None,
    ):
        """
        Helper method for bulk_create() to insert objs one batch at a time.
        """
        if ASYNC_TRUTH_MARKER:

            connection = async_connections.get_connection(self.db)
        else:
            connection = connections[self.db]
        ops = connection.ops
        max_batch_size = max(ops.bulk_batch_size(fields, objs), 1)
        batch_size = min(batch_size, max_batch_size) if batch_size else max_batch_size
        inserted_rows = []
        bulk_return = connection.features.can_return_rows_from_bulk_insert
        for item in [objs[i : i + batch_size] for i in range(0, len(objs), batch_size)]:
            if bulk_return and (
                on_conflict is None or on_conflict == OnConflict.UPDATE
            ):
                inserted_rows.extend(
                    await self._ainsert(
                        item,
                        fields=fields,
                        using=self.db,
                        on_conflict=on_conflict,
                        update_fields=update_fields,
                        unique_fields=unique_fields,
                        returning_fields=self.model._meta.db_returning_fields,
                    )
                )
            else:
                await self._ainsert(
                    item,
                    fields=fields,
                    using=self.db,
                    on_conflict=on_conflict,
                    update_fields=update_fields,
                    unique_fields=unique_fields,
                )
        return inserted_rows


class RawQuerySetOverrides:

    @generate_unasynced(sync_variant=RawQuerySet._prefetch_related_objects)
    async def _aprefetch_related_objects(self):
        await aprefetch_related_objects(self._result_cache, *self._prefetch_related_lookups)
        self._prefetch_done = True

    @staticmethod
    def _fetch_then_len(qs):
        qs._fetch_all()
        return len(qs._result_cache)

    @staticmethod
    async def _afetch_then_len(qs):
        # TODO link to _fetch_then_len
        # RawQuerySet helper
        await qs._afetch_all()
        return len(qs._result_cache)
