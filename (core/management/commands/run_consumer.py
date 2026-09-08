from django.core.management.base import BaseCommand
from core.kafka import run_simple_consumer

class Command(BaseCommand):
    help = "Starts the background Kafka consumer worker for database CRUD events."

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Initializing consumer engine..."))
        try:
            run_simple_consumer()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("\nConsumer worker stopped cleanly."))
