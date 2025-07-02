from django.db import models

from .base_model import BaseModel


class Dataset(BaseModel):
    """
    Model representing a dataset with experimental conditions and citation information.
    """

    name = models.CharField(
        max_length=50,
        blank=False,
        null=False,
        unique=True,
        db_index=True,
        help_text=(
            "CharField with a max length of 50, representing the name of the dataset"
        ),
    )
    assay = models.ForeignKey(
        "Assay",
        on_delete=models.CASCADE,
        related_name="datasets",
        help_text=(
            "ForeignKey to the Assay model, representing the assay of the dataset"
        ),
    )
    conditions = models.JSONField(
        default=dict,
        help_text=(
            "Stores the possible conditions (genetic or otherwise). "
            'Also stores fields like "individual" or "strain" if those '
            "are experimental properties"
        ),
    )
    citation = models.CharField(
        max_length=400,
        default="ask_admin",
        help_text=(
            "CharField with a max length of 400, representing the full "
            "citation used for UI tooltips"
        ),
    )
    url = models.URLField(
        blank=True,
        help_text=(
            "URLField representing the URL to the publication, used to "
            "make the name a hyperlink"
        ),
    )
    year = models.IntegerField(
        blank=False,
        null=False,
        help_text=(
            "IntegerField representing the publication year for sorting and display"
        ),
    )
    description = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing the "
            "optional longer description of the dataset"
        ),
    )
    notes = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing any notes "
            "about the dataset"
        ),
    )

    def __str__(self):
        return self.name

    class Meta:
        ordering = ["-year", "name"]
