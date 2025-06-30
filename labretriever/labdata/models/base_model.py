from django.conf import settings
from django.db import models


class BaseModel(models.Model):
    """
    An abstract base model that includes common fields for tracking the user
    who uploaded the data, the date of uploading, and the last modification
    date and user who made the modification.

    Inherit from this class to provide these common fields for other models.

    :ivar uploader: ForeignKey to the user model, representing the user who
        uploaded the data.
    :ivar uploadDate: DateField, automatically set to the date the object was
        created.
    :ivar modified: DateTimeField, automatically set to the current date and
        time when the object is updated. Note that this field is only
        updated when the object is saved using the save() method, not when
        using queryset.update().
    :ivar modifier: ForeignKey to the user model, representing the user who
        last modified the data.

    Example usage::

        from django.db import models
        from .base_model import BaseModel

        class MyModel(BaseModel):
            field1 = models.CharField(max_length=100)
            field2 = models.IntegerField()
    """

    uploader = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="%(class)s_uploader",
    )
    upload_date = models.DateField(auto_now_add=True)
    modifier = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="%(class)s_modifier",
    )
    # record when the record is modified
    # note: this only updates with model.save(), not queryset.update
    #       see docs -- may need to write some code to update this field
    #       for update statement when only certain fields are changed?
    modified_date = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True
