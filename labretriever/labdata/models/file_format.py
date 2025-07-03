from django.db import models

from .base_model import BaseModel


class FileFormat(BaseModel):
    """
    Model representing different file formats and their validation methods.
    """

    fileformat = models.CharField(
        max_length=20,
        blank=False,
        null=False,
        unique=True,
        help_text='Short name of the file format, e.g., "bed", "bam", "vcf"',
    )
    format_type = models.CharField(
        max_length=20,
        blank=False,
        null=False,
        default="text",
        help_text='Type of format: "text", "binary", "compressed", etc.',
    )
    validator = models.ForeignKey(
        "FileValidator",
        on_delete=models.CASCADE,
        related_name="fileformats",
        blank=True,
        null=True,
        help_text=(
            "ForeignKey to the FileValidator model, representing the "
            "validator of the file format"
        ),
    )
    description = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing the "
            "description of the file format"
        ),
    )
    notes = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing any notes "
            "about the file format"
        ),
    )

    def __str__(self):
        return self.fileformat

    class Meta:
        ordering = ["fileformat"]
