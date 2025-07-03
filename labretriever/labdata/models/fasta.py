from django.core.exceptions import ValidationError
from django.db import models

from .base_model import BaseModel


class Fasta(BaseModel):
    """
    Model representing FASTA files for genomes, transcriptomes, proteomes, primers, etc.
    """

    name = models.CharField(
        max_length=50,
        blank=False,
        null=False,
        help_text=(
            "CharField with a max length of 50, representing the name of the FASTA file"
        ),
    )
    type = models.CharField(
        max_length=20,
        blank=False,
        help_text='"transcriptome", "proteome", "primers", etc.',
    )
    organism = models.ForeignKey(
        "Organism",
        on_delete=models.CASCADE,
        related_name="fasta",
        help_text=(
            "ForeignKey to the Organism model, representing the organism "
            "of the FASTA file"
        ),
    )
    genome = models.ForeignKey(
        "Genome",
        on_delete=models.CASCADE,
        related_name="fasta",
        help_text=(
            "ForeignKey to the Genome model, representing the genome of the FASTA file"
        ),
    )
    version = models.CharField(
        max_length=100,
        default="none",
        help_text=(
            "CharField with a max length of 100, representing the version "
            "of the FASTA file"
        ),
    )
    source_url = models.URLField(
        blank=True,
        help_text=("URLField representing the URL to the source of the FASTA file"),
    )
    description = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing the "
            "description of the FASTA file"
        ),
    )
    fasta = models.FileField(
        upload_to="fasta/",
        help_text=(
            "FileField representing the FASTA file, stored in the 'fasta/' directory"
        ),
    )
    md5sum_fasta = models.CharField(
        max_length=32,
        blank=True,
        unique=True,
        help_text="CharField representing the MD5 checksum of the FASTA file",
    )
    # TODO: this should be a async offloaded task to generate the FAI file
    fai = models.FileField(
        upload_to="fasta/",
        blank=True,
        help_text=(
            "FileField representing the FASTA index file (FAI), "
            "stored in the 'fasta/' directory"
        ),
    )
    md5sum_fai = models.CharField(
        max_length=32,
        blank=True,
        help_text="CharField representing the MD5 checksum of the FAI file",
    )
    fileformat = models.ForeignKey(
        "FileFormat",
        on_delete=models.CASCADE,
        related_name="fasta",
        help_text=(
            "ForeignKey to the FileFormat model, representing the format "
            "of the FASTA file"
        ),
    )

    def __str__(self):
        return f"{self.name} ({self.type}) - {self.organism}"

    class Meta:
        ordering = ["organism", "type", "name"]

    def clean(self):
        super().clean()
        if self.file and not self.md5sum_file:
            raise ValidationError(
                {"md5sum_file": "MD5 checksum must be provided for the FASTA file."},
            )
        if self.fai and not self.md5sum_fai:
            raise ValidationError(
                {"md5sum_fai": "MD5 checksum must be provided for the FAI file."},
            )
