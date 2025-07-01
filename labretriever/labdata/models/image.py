from django.db import models

from .base_model import BaseModel


class Image(BaseModel):
    """
    Model representing image files associated with samples.
    """

    sample = models.ForeignKey(
        "Sample",
        on_delete=models.CASCADE,
        related_name="images",
        help_text=(
            "ForeignKey to the Sample model, representing the sample of the image"
        ),
    )
    file = models.FileField(upload_to="images/")
    notes = models.CharField(
        max_length=1000,
        default="none",
        help_text=(
            "CharField with a max length of 1000, representing any notes "
            "about the image"
        ),
    )

    def __str__(self):
        return f"Image for {self.sample}"

    class Meta:
        ordering = ["sample", "upload_date"]
