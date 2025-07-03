from django.db import models

from .base_model import BaseModel


class Assay(BaseModel):
    """
    Model representing different types of assays (e.g., callingcards,
    chipexo, overexpression, knockout).
    """

    name = models.CharField(
        max_length=20,
        blank=False,
        null=False,
        unique=True,
        help_text=(
            "CharField with a max length of 20, representing name of the "
            "assay used to generate the data"
        ),
    )
    description = models.CharField(
        max_length=1000,
        blank=False,
        null=False,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing the "
            "description of the assay"
        ),
    )

    def __str__(self):
        return self.name

    class Meta:
        ordering = ["name"]
