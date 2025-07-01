from django.db import models

from .base_model import BaseModel


class Genome(BaseModel):
    """
    Model representing a genome version for a specific organism.
    """

    organism = models.ForeignKey(
        "Organism",
        on_delete=models.CASCADE,
        related_name="genomes",
        help_text=(
            "ForeignKey to the Organism model, representing the organism of the genome"
        ),
    )
    version = models.CharField(
        max_length=100,
        blank=False,
        null=False,
        default="none",
        help_text=(
            "CharField with a max length of 100, representing the version of the genome"
        ),
    )
    notes = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing any notes "
            "about the genome"
        ),
    )

    def __str__(self):
        return f"{self.organism.name} {self.version}"

    class Meta:
        ordering = ["organism__name", "version"]
        unique_together = ["organism", "version"]
