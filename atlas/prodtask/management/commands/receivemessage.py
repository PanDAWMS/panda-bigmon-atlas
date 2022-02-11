from django.core.management.base import BaseCommand

from atlas.messaging.manager import start_bunch
from atlas.settings.messaging import IDDS_PRODUCTION_CONFIG


class Command(BaseCommand):
    help = "Start internal PUB logic"


    def handle(self, *args, **options):
        self.stdout.write(f"Provided parameters: {options}")

        # source_destination = options.get("source_destination")
        # callback_function = options.get("callback_function")

        self.stdout.write("Calling internal service to consume messages")
        config = IDDS_PRODUCTION_CONFIG['connection']
        start_bunch(IDDS_PRODUCTION_CONFIG['queue'],'atlas.special_workflows.views.idds_recive_message',config)

