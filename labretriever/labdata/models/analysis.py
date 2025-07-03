from django.db import models

from .base_model import BaseModel
from .file_model import FileModel


class Analysis(BaseModel, FileModel):
    """
    Model representing analysis results.

    Note: Should there be another table that names analysis?
    What happens when there are multiple analyses of the same input data
    but with different parameters that we do want to store?
    There will need to be an analysis name of some sort, probably
    a table like FileFormat.

    Problem: What happens when you have multiples of the same type on the
    same data, e.g., with different parameters?
    """

    type = models.CharField(
        max_length=200,
        blank=False,
        null=False,
        help_text="What kind of analysis is this?",
    )
    parameters = models.TextField(
        help_text="TextField representing the parameters of the analysis",
    )
    input = models.JSONField(
        help_text=(
            "Keys must be a Table in the database, values are a list of "
            "ids from that table"
        ),
        blank=False,
        null=False,
    )

    def __str__(self):
        return f"{self.type} analysis"

    class Meta:
        ordering = ["type", "upload_date"]
        verbose_name_plural = "Analyses"
