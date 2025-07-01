from django.db import models

from .base_model import BaseModel

MAX_LIBRARIES_DISPLAY = 3


class AnnotatedFeatures(BaseModel):
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
        related_name="annotated_features",
        help_text=(
            "ForeignKey to the GenomicFeatures model, representing the "
            "genomic features of the annotated features file"
        ),
    )
    library = models.ManyToManyField(
        "Library",
        related_name="annotated_features",
        help_text=(
            "ManyToManyField to the Library model, representing the "
            "libraries of the annotated features file"
        ),
    )
    fileformat = models.ForeignKey(
        "FileFormat",
        on_delete=models.CASCADE,
        related_name="annotated_features",
        help_text=(
            "ForeignKey to the FileFormat model, representing the format "
            "of the annotated features file"
        ),
    )
    file = models.FileField(
        upload_to="annotated_features/",
        help_text="FileField representing the annotated features file",
    )
    notes = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing any notes "
            "about the annotated features file"
        ),
    )

    def __str__(self):
        libraries = ", ".join(
            [str(lib) for lib in self.library.all()[:MAX_LIBRARIES_DISPLAY]],
        )
        if self.library.count() > MAX_LIBRARIES_DISPLAY:
            libraries += "..."
        return f"AnnotatedFeatures ({self.genomicfeatures}) for {libraries}"

    class Meta:
        ordering = ["genomicfeatures", "upload_date"]
        verbose_name_plural = "Annotated features"
