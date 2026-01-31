import random
from django.core.management.base import BaseCommand
from biz.models import Client, Invoice


class Command(BaseCommand):
    def handle(self, *args, **options):
        start = Client.objects.count() + 1
        for i in range(10):
            client = Client.objects.create(name=f"Client #{start + i}")
            for j in range(random.randint(5, 10)):
                Invoice.objects.create(
                    client=client,
                    reference=f"INV-{client.pk}-{j+1}",
                    total=random.randint(100, 10000),
                )
        self.stdout.write(f"Created 10 clients starting from #{start}")
