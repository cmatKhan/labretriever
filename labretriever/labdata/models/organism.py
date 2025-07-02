from django.db import models

from .base_model import BaseModel


class Organism(BaseModel):
    """
    Model representing an organism with its scientific name.
    """

    name = models.CharField(
        max_length=50,
        blank=False,
        null=False,
        unique=True,
        help_text=(
            "CharField with a max length of 50, representing the "
            "scientific name of the organism"
        ),
    )
    notes = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing any notes "
            "about the organism"
        ),
    )

    def __str__(self):
        return self.name

    class Meta:
        ordering = ["name"]
