from django.db import models

from .base_model import BaseModel


class Service(BaseModel):
    name = models.CharField(max_length=100, unique=True)
    command_template = models.TextField(
        help_text="Shell command with placeholders. E.g., 'bedtools genomecov {params}'",
    )
    description = models.TextField(blank=True)
    default_output_name = models.CharField(max_length=100, default="output.tar.gz")

    def __str__(self):
        return self.name
