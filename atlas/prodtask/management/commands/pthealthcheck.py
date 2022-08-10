from django.core.management.base import BaseCommand, CommandError
import time

from django_celery_beat.models import PeriodicTask
from django.utils import timezone
from datetime import timedelta

from atlas.prodtask.views import send_alarm_message


class Command(BaseCommand):
    args = 'None'
    help = 'Check celery beat health'

    def handle(self, *args, **options):
        if not args:
            try:
                try:
                    last_executed_task = PeriodicTask.objects.all().order_by('-last_run_at')[0]
                except Exception as e:
                    send_alarm_message('Alarm: the celery beat health check problem',
                              f'Celery beat health check problem {e}')
                    raise e
                if (timezone.now() - last_executed_task.last_run_at) < timedelta(hours=3):
                    send_alarm_message('Alarm: the celery beat is stuck',
                                       f'Celery beat last updated {last_executed_task.last_run_at}')
            except Exception as e:
                raise CommandError('Some problem during alarm mail sending check: %s'%e)
