#!/usr/bin/env python3
from functools import wraps

from typing import Any
from dataclasses import dataclass
import inspect


def get_owning_class(func):
    if inspect.ismethod(func):
        func = func.__func__
    module = inspect.getmodule(func)
    qualname = func.__qualname__  # e.g. "MyClass.method"
    cls_name, meth_name = qualname.split(".", maxsplit=1)
    if not module:
        return None
    result = getattr(module, cls_name)
    if getattr(result, meth_name) != func:
        return None
    return result


@dataclass
class RegistryItem:
    label: str
    our_copy: Any
    original_copy: Any
    patch: bool

    def apply_patch(self):
        if not self.patch:
            return
        assert self.label == "generate_unasynced"
        owning_cls = get_owning_class(self.original_copy)
        print(
            "Patching ",
            owning_cls,
            "with ",
            self.our_copy,
            "(",
            self.our_copy.__name__,
            ")",
        )
        setattr(owning_cls, self.our_copy.__name__, self.our_copy)


_registry: list[RegistryItem] = []


def sync_methods() -> list[RegistryItem]:
    return [r for r in _registry if r.label == "from_codegen"]


def async_methods() -> list[RegistryItem]:
    return [r for r in _registry if r.label == "generate_unasynced"]


def from_codegen(original: Any):
    def register(meth):
        _registry.append(
            RegistryItem(
                label="from_codegen", our_copy=meth, original_copy=original, patch=False
            )
        )
        return meth

    return register


TRACKING_UNASYNCED = True
if TRACKING_UNASYNCED:

    def generate_unasynced(sync_variant=None, async_unsafe=False, patch=True):
        def wrapper(f):
            _registry.append(
                RegistryItem(
                    label="generate_unasynced",
                    our_copy=f,
                    original_copy=sync_variant,
                    patch=patch,
                )
            )

            @wraps(f)
            def wrapped(*args, **kwargs):
                assert False, "IN NEW VALUE"
                return f(*args, **kwargs)

            return wrapped

        return wrapper

else:

    def generate_unasynced(sync_variant=None, async_unsafe=False, patch=True):
        """
        This indicates we should unasync this function/method

        async_unsafe indicates whether to add the async_unsafe decorator
        """

        def wrapper(f):
            _registry.append(
                RegistryItem(
                    label="generate_unasynced",
                    our_copy=f,
                    original_copy=sync_variant,
                    patch=patch,
                )
            )

            return f

        return wrapper
