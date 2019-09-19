"""Create CLI utility for adding Validation Scenarios to Combine."""
from django.core.management.base import BaseCommand
import requests
from core.models import Transformation


class Command(BaseCommand):
    """Generic Class for running command."""


    def add_arguments(self, parser):
        """Arguments passed to management CLI & used to create Transformations."""
        parser.add_argument('name', type=str)
        parser.add_argument('type', choices=['python', 'xslt', 'openrefine'], type=str)
        parser.add_argument('payload_url', type=str)


    def handle(self, **options):
        """Create or Update Transformations with CLI Args."""
        try:
            payload = requests.get(options['payload_url']).text
        except Exception as e:
            print(e)
            return

        try:
            transformation = Transformation.objects.get(name=options['name'])
        except:
            transformation = Transformation(
                name=options['name'],
                payload=payload,
                transformation_type=options['type'],
                filepath=None
            )

        transformation.save()
