from django.db import models

from .base_model import BaseModel


class Sample(BaseModel):
    """
    Model representing a biological sample from a dataset.
    Note: This isn't equivalent to a single "mouse", e.g., because if that mouse
    was sampled multiple times over a timecourse, then those samples should each
    be a single biosample.
    """

    tissue = models.CharField(
        max_length=200,
        blank=True,
        help_text=(
            "CharField with a max length of 200, representing the tissue of the sample"
        ),
    )
    dataset = models.ForeignKey(
        "Dataset",
        on_delete=models.CASCADE,
        related_name="sample",
        help_text=(
            "ForeignKey to the Dataset model, representing the dataset of the sample"
        ),
    )
    condition = models.JSONField(
        default=dict,
        help_text=(
            "Key/value pairs where the keys are in the associated Dataset.conditions"
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
        tissue_str = f" ({self.tissue})" if self.tissue else ""
        return f"{self.dataset.name} - {self.batch}{tissue_str}"

    class Meta:
        ordering = ["dataset", "date", "batch"]
