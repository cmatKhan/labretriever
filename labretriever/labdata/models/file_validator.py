from django.db import models

from .base_model import BaseModel


class FileValidator(BaseModel):
    """
    Model representing different file validation methods.
    """

    name = models.CharField(
        max_length=50,
        blank=False,
        null=False,
        unique=True,
        help_text='Unique identifier, e.g., "pysam_bam", "bed_schema", "remote_vcf"',
    )
    validator_type = models.CharField(
        max_length=20,
        blank=False,
        null=False,
        help_text='Type of validator: "http", "python", "cli", etc.',
    )
    endpoint = models.TextField(
        blank=False,
        null=False,
        help_text=(
            "URL, import path, or command depending on type. E.g., "
            '"validators.pysam_bam", "https://.../validate", or '
            '"/usr/bin/bedtools"'
        ),
    )
    description = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "Optional human-readable explanation of what this validator "
            "does or validates."
        ),
    )
    active = models.BooleanField(
        default=True,
        help_text="Used to disable validators without deleting them.",
    )
    notes = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing any notes "
            "about the file validator"
        ),
    )

    def __str__(self):
        return self.name

    class Meta:
        ordering = ["name"]
