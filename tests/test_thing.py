#!/usr/bin/env python3
from biz.views import aget_client_count_orm
import pytest
from biz.models import Client

from django_async_backend.db import async_connections


def test_thing():
    assert 1 == 1


@pytest.mark.django_db
def test_client_name_length():
    client = Client.objects.create(name="foo")
    assert client.name_length() == 3


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_aget_client_count_orm():
    assert (await aget_client_count_orm(0.1)) == 0


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_connection_usages():
    async with async_connections["default"].cursor() as cursor:
        await cursor.execute("SELECT COUNT(*) FROM biz_client")
        val = await cursor.fetchone()
        assert val == (0,)
