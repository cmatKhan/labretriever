from django.db import models

from .base_model import BaseModel

MAX_LIBRARIES_DISPLAY = 3


class QCFiles(BaseModel):
    """
    Model representing quality control files.
    Is this actually an Analysis datatype?
    """

    file = models.FileField(
        upload_to="qc_files/",
        help_text="FileField representing the QC file",
    )
    library = models.ManyToManyField(
        "Library",
        related_name="qc_files",
        help_text=(
            "ManyToManyField to the Library model, representing the "
            "libraries of the QC file"
        ),
    )
    fileformat = models.ForeignKey(
        "FileFormat",
        on_delete=models.CASCADE,
        related_name="qc_files",
        help_text=(
            "ForeignKey to the FileFormat model, representing the format of the QC file"
        ),
    )
    notes = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing any notes "
            "about the QC file"
        ),
    )

    def __str__(self):
        libraries = ", ".join(
            [str(lib) for lib in self.library.all()[:MAX_LIBRARIES_DISPLAY]],
        )
        if self.library.count() > MAX_LIBRARIES_DISPLAY:
            libraries += "..."
        return f"QC Files for {libraries}"

    class Meta:
        ordering = ["upload_date"]
        verbose_name_plural = "QC files"
