from django.db import models

from .base_model import BaseModel


class GenomicFeatures(BaseModel):
    """
    Model representing genomic features files for a specific genome.
    """

    name = models.CharField(
        max_length=50,
        blank=False,
        null=False,
        unique=True,
        help_text=(
            "CharField with a max length of 50, representing the name "
            "of the genomic features file"
        ),
    )
    genome = models.ForeignKey(
        "Genome",
        on_delete=models.CASCADE,
        related_name="genomicfeatures",
        help_text=(
            "ForeignKey to the Genome model, representing the genome "
            "of the genomic features file"
        ),
    )
    version = models.CharField(
        max_length=100,
        blank=False,
        null=False,
        default="none",
        help_text=(
            "CharField with a max length of 100, representing the version "
            "of the genomic features file"
        ),
    )
    fileformat = models.ForeignKey(
        "FileFormat",
        on_delete=models.CASCADE,
        related_name="genomicfeatures",
        help_text=(
            "ForeignKey to the FileFormat model, representing the file "
            "format of the genomic features file"
        ),
    )
    file = models.FileField(upload_to="genomicfeatures/")
    md5sum = models.CharField(
        max_length=32,
        blank=True,
        help_text=(
            "CharField with a max length of 32, representing the MD5 checksum "
            "of the genomic features file"
        ),
    )
    source_url = models.URLField(
        blank=True,
        help_text=(
            "URLField representing the URL to the source of the genomic features file"
        ),
    )

    def __str__(self):
        return f"{self.name} ({self.genome})"

    class Meta:
        ordering = ["genome", "name", "version"]
        verbose_name_plural = "Genomic features"
