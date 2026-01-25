from django.db.models.base import Model
from django_async_patchup.registry import just_patch


@just_patch(onto=Model)
@classmethod
def __init_subclass__(cls, /, *args, **kwargs):

    super(Model).__init_subclass__(**kwargs)
    # the following are pairings of sync and async variants of model methods
    # if a subclass overrides one of these without overriding the other, then
    # we should make the other one fallback to using the overriding one
    #
    # for example: if I override save, then asave should call into my overridden
    # save, instead of the default asave (which does it's own thing)
    method_pairings = [
        ("save", "asave"),
    ]

    for sync_variant, async_variant in method_pairings:
        sync_defined = sync_variant in cls.__dict__
        async_defined = async_variant in cls.__dict__
        if sync_defined and not async_defined:
            # async should fallback to sync
            setattr(cls, async_variant, sync_to_async(getattr(cls, sync_variant)))
        if not sync_defined and async_defined:
            # sync should fallback to async
            setattr(cls, sync_variant, async_to_sync(getattr(cls, async_variant)))
