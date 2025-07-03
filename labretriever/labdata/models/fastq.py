from django.db import models

from .base_model import BaseModel

MAX_LIBRARIES_DISPLAY = 3


class Fastq(BaseModel):
    """
    Model representing FASTQ files with sequencing metadata.
    Note: How to store R1 and R2 or interleaved?
    """

    library = models.ManyToManyField(
        "Library",
        related_name="fastq",
        help_text=(
            "ManyToManyField to the Library model, representing the "
            "libraries of the FASTQ file"
        ),
    )
    fileformat = models.ForeignKey(
        "FileFormat",
        on_delete=models.CASCADE,
        related_name="fastq",
        help_text=(
            "ForeignKey to the FileFormat model, representing the format "
            "of the FASTQ file"
        ),
    )
    platform = models.CharField(
        max_length=100,
        blank=False,
        null=False,
        default="none",
        help_text=(
            "CharField with a max length of 100, representing the platform "
            "of the FASTQ file"
        ),
    )
    read_structure = models.CharField(
        max_length=200,
        blank=False,
        null=False,
        default="none",
        help_text=(
            "e.g., 16bp cell barcode + 10bp UMI + 50bp RNA (e.g., R1: 16C10U24T)"
        ),
    )
    barcode_whitelist = models.FileField(
        upload_to="barcode_whitelists/",
        help_text=("FileField representing the barcode whitelist of the FASTQ file"),
    )
    sample_index = models.CharField(
        max_length=100,
        blank=True,
        help_text=(
            "CharField with a max length of 100, representing the sample "
            "index of the FASTQ file"
        ),
    )
    flowcell = models.CharField(
        max_length=100,
        blank=False,
        null=False,
        help_text=(
            "CharField with a max length of 100, representing the flowcell "
            "of the FASTQ file"
        ),
    )
    lane = models.IntegerField(
        help_text="IntegerField representing the lane of the FASTQ file",
    )
    interleaved = models.BooleanField(
        default=False,
        help_text=("BooleanField representing whether the FASTQ file is interleaved"),
    )
    r1 = models.FileField(
        upload_to="fastq/",
        help_text="FileField representing the R1 of the FASTQ file",
    )
    r1_md5sum = models.CharField(
        max_length=32,
        blank=True,
        help_text=(
            "CharField with a max length of 32, representing the MD5 checksum "
            "of the R1 FASTQ file"
        ),
        unique=True,
    )
    r2 = models.FileField(
        upload_to="fastq/",
        blank=True,
        null=True,
        help_text="FileField representing the R2 of the FASTQ file",
    )
    r2_md5sum = models.CharField(
        max_length=32,
        blank=True,
        help_text=(
            "CharField with a max length of 32, representing the MD5 checksum "
            "of the R2 FASTQ file"
        ),
        unique=True,
    )
    notes = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing any notes "
            "about the FASTQ file"
        ),
    )

    def __str__(self):
        libraries = ", ".join(
            [str(lib) for lib in self.library.all()[:MAX_LIBRARIES_DISPLAY]],
        )
        if self.library.count() > MAX_LIBRARIES_DISPLAY:
            libraries += "..."
        return f"FASTQ for {libraries} - {self.flowcell}:{self.lane}"

    class Meta:
        ordering = ["flowcell", "lane"]
