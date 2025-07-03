from django.db import models

from .base_model import BaseModel


class Library(BaseModel):
    """
    Model representing sequencing libraries prepared from samples.
    """

    sample = models.ForeignKey(
        "Sample",
        on_delete=models.CASCADE,
        related_name="library",
        help_text=(
            "ForeignKey to the Sample model, representing the sample of the library"
        ),
    )
    date = models.DateTimeField()
    batch = models.CharField(
        max_length=20,
        default="undefined",
        help_text=(
            "A batch identifier for a set of data, eg the run number "
            "in the case of calling cards"
        ),
        db_index=True,
    )

    def __str__(self):
        return f"Library {self.batch} for {self.sample}"

    class Meta:
        ordering = ["sample", "date", "batch"]
        verbose_name_plural = "Libraries"
