from django.db import models


class Client(models.Model):
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name

    def name_length(self):
        return len(self.name)


class Invoice(models.Model):
    client = models.ForeignKey(
        Client, on_delete=models.CASCADE, related_name="invoices"
    )
    reference = models.CharField(max_length=100)
    total = models.DecimalField(max_digits=10, decimal_places=2)

    def __str__(self):
        return f"{self.reference} - {self.total}"
