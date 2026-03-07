from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("biz", "0002_person_employee"),
    ]

    operations = [
        migrations.AddField(
            model_name="client",
            name="metadata",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
