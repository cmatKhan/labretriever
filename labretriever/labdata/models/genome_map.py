from django.db import models

from .base_model import BaseModel

MAX_LIBRARIES_DISPLAY = 3


class GenomeMap(BaseModel):
    """
    Model representing genome mapping files.
    """

    library = models.ManyToManyField(
        "Library",
        related_name="genomemap",
        help_text=(
            "ManyToManyField to the Library model, representing the "
            "libraries of the genome map file"
        ),
    )
    # This field is used to track the relationships between genome maps.
    # Eg, a bam is a GenomeMap file type, and if that bam is processed with bedtools
    # genomecov, then the output is also a GenomeMap file type
    parents = models.ManyToManyField(
        "self",
        symmetrical=False,
        related_name="derived",
        blank=True,
        help_text="Other GenomeMaps this one was derived from.",
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
