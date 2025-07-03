from django.db import models

from .base_model import BaseModel


class Image(BaseModel):
    """
    Model representing image files associated with samples.
    """

    sample = models.ForeignKey(
        "Sample",
        on_delete=models.CASCADE,
        related_name="image",
        help_text=(
            "ForeignKey to the Sample model, representing the sample of the image"
        ),
    )
    file = models.FileField(upload_to="images/")
    md5sum = models.CharField(
        max_length=32,
        blank=True,
        help_text=(
            "CharField with a max length of 32, representing the MD5 checksum "
            "of the image file"
        ),
    )

    def __str__(self):
        return f"Image for {self.sample}"

    class Meta:
        ordering = ["sample", "upload_date"]
