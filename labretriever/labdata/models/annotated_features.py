from django.core.exceptions import ValidationError
from django.db import models

from .base_model import BaseModel
from .file_model import FileModel

MAX_LIBRARIES_DISPLAY = 3


class AnnotatedFeatures(BaseModel, FileModel):
    """
    Model representing annotated features files.
    For AnnotatedFeatures, this now eliminates the need for the
    BindingConcatenated table. Instead of having another table to refer to,
    a "promotersetsig" (which is an AnnotatedFeatures datatype) that combines
    multiple replicates would simply refer to those replicates library_id in
    the ManyToMany foreign key.
    """

    genomicfeatures = models.ForeignKey(
        "GenomicFeatures",
        on_delete=models.CASCADE,
        related_name="annotatedfeatures",
        help_text=(
            "ForeignKey to the GenomicFeatures model, representing the "
            "genomic features of the annotated features file"
        ),
    )
    library = models.ManyToManyField(
        "Library",
        related_name="annotatedfeatures",
        help_text=(
            "ManyToManyField to the Library model, representing the "
            "libraries of the annotated features file"
        ),
    )

    def __str__(self):
        libraries = ", ".join(
            [str(lib) for lib in self.library.all()[:MAX_LIBRARIES_DISPLAY]],
        )
        if self.library.count() > MAX_LIBRARIES_DISPLAY:
            libraries += "..."
        return f"AnnotatedFeatures ({self.genomicfeatures}) for {libraries}"

    def clean(self):
        super().clean()
        if self.file and not self.md5sum:
            raise ValidationError(
                {
                    "md5sum": "MD5 checksum must be provided if a file is uploaded.",
                },
            )

    class Meta:
        ordering = ["genomicfeatures", "upload_date"]
        verbose_name_plural = "Annotated features"
