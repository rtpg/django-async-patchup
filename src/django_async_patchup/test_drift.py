#!/usr/bin/env python3
import inspect
from django_async_patchup.registry import RegistryItem, sync_methods
from django_async_patchup import setup


def test_codegen_matches():
    print("Setup...")
    setup()
    print("Checking...")
    items = sync_methods()
    print(f"{len(items)} to check...")
    for item in items:
        check_item(item)


def check_item(item: RegistryItem):
    # we just want to make sure the source looks the same
    original_source = "".join(inspect.getsourcelines(item.original_copy)[0])
    # we strip the first line (@from_codegen) from the source to compare
    generated_source = "".join(inspect.getsourcelines(item.our_copy)[0][1:])
    # TODO make this a lot nicer
    assert original_source == generated_source
