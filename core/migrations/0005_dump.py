# Generated by Django 3.0.4 on 2020-03-31 17:13

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0004_auto_20200324_2143'),
    ]

    operations = [
        migrations.CreateModel(
            name='Dump',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('added_ts', models.DateTimeField(auto_now_add=True)),
                ('stats_type', models.CharField(default='', max_length=255)),
                ('dump', models.TextField(default='')),
            ],
        ),
    ]