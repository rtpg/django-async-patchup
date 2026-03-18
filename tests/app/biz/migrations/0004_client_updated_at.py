from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("biz", "0003_client_metadata"),
    ]

    operations = [
        migrations.AddField(
            model_name="client",
            name="updated_at",
            field=models.DateTimeField(auto_now=True),
        ),
    ]
