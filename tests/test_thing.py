#!/usr/bin/env python3
from biz.views import aget_client_count_orm
import pytest
import pytest_asyncio
from biz.models import Client

from django_async_backend.db import async_connections


@pytest_asyncio.fixture(autouse=True)
async def truncate_biz_tables(django_db_setup):
    """Ensure biz tables are clean before each test in this module."""
    # XXX I feel like we shouldn't need to do this??
    async with async_connections["default"].cursor() as cursor:
        await cursor.execute(
            "TRUNCATE TABLE biz_invoice, biz_client RESTART IDENTITY CASCADE"
        )


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
