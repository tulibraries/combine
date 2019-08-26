"""Create CLI utility for adding Validation Scenarios to Combine."""
from django.core.management.base import BaseCommand
import requests
from core.models import ValidationScenario


class Command(BaseCommand):
    """Generic Class for running command."""

    def add_arguments(self, parser):
        """Arguments passed to management CLI & used to create Validation Scenarios."""
        parser.add_argument('name', type=str)
        parser.add_argument('type', choices=['sch','python','es_query','xsd'], type=str)
        parser.add_argument('default_run', type=bool)
        parser.add_argument('payload_url', type=str)

    def handle(self, **options):
        """Create or Update Validation Scenario with CLI Args."""
        try:
            payload = requests.get(options['payload_url']).text
        except Exception as e:
            print(e)
            return

        try:
            validation_scenario = ValidationScenario.objects.get(name=options['name'])
        except:
            validation_scenario = ValidationScenario(
                name=options['name'],
                payload=payload,
                validation_type=options['type'],
                default_run=options['default_run'],
                filepath=None
            )

        validation_scenario.save()
