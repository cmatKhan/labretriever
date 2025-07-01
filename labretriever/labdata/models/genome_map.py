from django.db import models

from .base_model import BaseModel

MAX_LIBRARIES_DISPLAY = 3


class GenomeMap(BaseModel):
    """
    Model representing genome mapping files.
    """

    library = models.ManyToManyField(
        "Library",
        related_name="genome_maps",
        help_text=(
            "ManyToManyField to the Library model, representing the "
            "libraries of the genome map file"
        ),
    )
    fileformat = models.ForeignKey(
        "FileFormat",
        on_delete=models.CASCADE,
        related_name="genome_maps",
        help_text=(
            "ForeignKey to the FileFormat model, representing the format "
            "of the genome map file"
        ),
    )
    file = models.FileField(upload_to="genome_maps/")
    notes = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing any notes "
            "about the genome map file"
        ),
    )

    def __str__(self):
        libraries = ", ".join(
            [str(lib) for lib in self.library.all()[:MAX_LIBRARIES_DISPLAY]],
        )
        if self.library.count() > MAX_LIBRARIES_DISPLAY:
            libraries += "..."
        return f"GenomeMap for {libraries}"

    class Meta:
        ordering = ["upload_date"]
