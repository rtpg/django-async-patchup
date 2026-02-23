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


class Person(models.Model):
    """Base model for multi-table inheritance tests."""
    first_name = models.CharField(max_length=100)

    def __str__(self):
        return self.first_name


class Employee(Person):
    """Child model to exercise MTI async saves (_asave_parents path)."""
    department = models.CharField(max_length=100)

    def __str__(self):
        return f"{self.first_name} ({self.department})"
