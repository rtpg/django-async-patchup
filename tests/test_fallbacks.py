#!/usr/bin/env python3


from asgiref.sync import async_to_sync
from django.db import transaction
import pytest
from biz.models import Client


async def get_name_via_async(c):
    refetched_c = await Client.objects.aget(id=c.id)
    return refetched_c.name


@pytest.mark.django_db
def test_fallback_behavior():

    c = Client.objects.create(name="1")
    with transaction.atomic():
        Client.objects.update(name="2")
        # the client's name is 2 in this atomic block
        # and should be visible as such
        assert async_to_sync(get_name_via_async)(c) == "2"
