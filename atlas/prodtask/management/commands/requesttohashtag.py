from django.core.management.base import BaseCommand, CommandError
import time

from atlas.prodtask.hashtag import hashtag_request_to_tasks


class Command(BaseCommand):
    args = '<request_id, request_id>'
    help = 'Save hashtags from request to tasks'

    def handle(self, *args, **options):
        self.stdout.write('Start hashtag from request to tasks at %s'%time.ctime())
        if not args:
            try:
                hashtag_request_to_tasks()
            except Exception as e:
                raise CommandError('Some problem during hashtag assign: %s'%str(e))
        self.stdout.write('Successfully finished request hashtag to tasks: %s'%time.ctime())