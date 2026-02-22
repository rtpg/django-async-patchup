#!/usr/bin/env python3
import difflib
import inspect
import textwrap

import libcst as cst
from libcst import Name

from django_async_patchup.codegen.async_helpers import UnasyncifyMethod
from django_async_patchup.registry import RegistryItem, sync_methods, unasynced_methods
from django_async_patchup import setup


def check_item(item: RegistryItem):
    # we just want to make sure the source looks the same
    original_source = "".join(inspect.getsourcelines(item.original_copy)[0])
    # we strip the first line (@from_codegen) from the source to compare
    generated_source = "".join(inspect.getsourcelines(item.our_copy)[0][1:])
    # TODO make this a lot nicer
    assert original_source == generated_source


def calculate_new_name(old_name: str) -> str:
    if old_name.startswith("test_async_"):
        return old_name.replace("test_async_", "test_", 1)
    if old_name.startswith("_a"):
        return "_" + old_name[2:]
    if old_name.startswith("a"):
        return old_name[1:]
    raise ValueError(f"Unknown name replacement pattern for {old_name}")


def transform_to_sync(async_func) -> str:
    """
    Given an async function decorated with @generate_unasynced,
    return the source code of the equivalent sync function.
    """
    source = textwrap.dedent(inspect.getsource(async_func))
    module = cst.parse_module(source)

    func = module.body[0]
    assert isinstance(func, cst.FunctionDef), f"Expected FunctionDef, got {type(func)}"

    # Rename: aXxx -> Xxx
    old_name = func.name.value
    new_name = calculate_new_name(old_name)

    # Strip the first decorator (@generate_unasynced(...)) and remove async
    func = func.with_changes(
        name=Name(new_name),
        asynchronous=None,
        decorators=func.decorators[1:],
    )

    # Apply the body transformer (removes await, renames calls, etc.)
    func = func.visit(UnasyncifyMethod())

    result = module.with_changes(body=[func])
    return result.code


def check_unasynced_item(item: RegistryItem):
    if item.original_copy is None:
        # No sync variant to compare against
        return

    generated = transform_to_sync(item.our_copy)
    original = textwrap.dedent(inspect.getsource(item.original_copy))

    if generated != original:
        diff = "".join(
            difflib.unified_diff(
                original.splitlines(keepends=True),
                generated.splitlines(keepends=True),
                fromfile="original (sync_variant)",
                tofile="generated (from async)",
            )
        )
        assert False, f"Mismatch for {item.our_copy.__name__}:\n{diff}"


def test_unasynced_matches(subtests):
    setup()
    items = unasynced_methods()
    for item in items:
        with subtests.test(msg=f"Method {item.our_copy.__name__}"):
            check_unasynced_item(item)
