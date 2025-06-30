import logging
from typing import Protocol
from typing import cast

from django.core.files.storage import default_storage

logger = logging.getLogger(__name__)


class HasPkProtocol(Protocol):  # pylint: disable=too-few-public-methods
    """
    from copilot: In Python's typing system, a `Protocol` is a way to define an
    interface that a class must adhere to. It's a way to say "any class that
    uses this protocol must have these methods or attributes". In this case,
    `HasPkProtocol` is a protocol that requires a `pk` attribute. The line
    `pk: Optional[int]` is saying that any class that uses this protocol
    must have a `pk` attribute, and that attribute can be an integer or
    `None`. When `GzipFileUploadWithIdMixin` inherits from `HasPkProtocol`,
    it's saying that `GzipFileUploadWithIdMixin` expects to be used with classes that
    have a `pk` attribute. This doesn't enforce anything at runtime, but it
    gives hints to static type checkers like mypy. So, when you use
    `GzipFileUploadWithIdMixin` as a mixin for a Django model, mypy will understand
    that the `pk` attribute is expected to be there, because Django models do
    have a `pk` attribute. This helps to eliminate the error messages you were
    seeing from mypy.
    """

    pk: int | None


class GzipFileUploadWithIdMixin:  # pylint: disable=too-few-public-methods
    """
    A mixin for models that have a file field that should be uploaded to a
    specific directory and renamed based on the instance's ID. This mixin
    requires that the model have a FileField with an `upload_to` argument

    Example usage:

    .. code-block:: python

        class Hu_s3(BaseModel, GzipFileUploadWithIdMixin):
        # Temporary upload path
        file = models.FileField(upload_to='temp', help_text="A gziped csv...")

        def save(self, *args, **kwargs):
            super().save(*args, **kwargs)
            self.update_file_name('file', 'hu', 'csv.gz')
            super().save(update_fields=['file'])

        # Other fields and methods...

    """

    def update_file_name(
        self,
        file_field_name: str,
        upload_dir: str,
        extension: str = "",
    ) -> None:
        """
        A utility method to update the file name based on the instance's ID.
        This method should be called in the `save` method of the model.

        :param file_field_name: The name of the file field to update.
        :type file_field_name: str
        :param upload_dir: The directory to upload the file to.
        :type upload_dir: str
        :param extension: The file extension to use. By default set to "", which
            will use the extension of the file name. If the file name does not
            have an extension that can be easily parsed out, `extension`
            will be set to `.txt.gz`.
        :type extension: str

        :return: None
        :rtype: None
        """
        try:
            # extract the extension, which will be eg .tsv.gz, from the file name
            file_name_parts = getattr(self, file_field_name).name.split(".")
        except AttributeError:
            logger.info(
                'No file field name provided. Skipping update_file_name for "%s"',
                self,
            )
        else:
            if extension:
                logger.debug("Using provided extension: %s", extension)
            else:
                extension = (
                    ".".join(file_name_parts[-2:])
                    if len(file_name_parts) > 1
                    else file_name_parts[1:]
                )
                if not extension:
                    logger.warning(
                        'Could not extract extension from file name "%s".'
                        "Setting to `.txt.gz`",
                        getattr(self, file_field_name).name,
                    )
                    extension = ".txt.gz"
            # Cast self to HasPkProtocol to assure mypy that self has a pk attribute
            self_with_pk = cast("HasPkProtocol", self)
            # raise AttributeError if self does not have a pk attribute
            if not self_with_pk.pk:
                msg = f"{self} does not have a pk attribute"
                raise AttributeError(msg)
            logger.debug(
                "Updating file name for %s to %s/%s.%s",
                self_with_pk,
                upload_dir,
                self_with_pk.pk,
                extension,
            )
            file_field = getattr(self, file_field_name, None)
            if file_field and self_with_pk.pk and file_field.name:
                # Define new filename with ID
                new_filename = f"{upload_dir}/{self_with_pk.pk}.{extension}"

                # Move and rename the file if it exists
                if default_storage.exists(file_field.name):
                    default_storage.save(new_filename, file_field)
                    default_storage.delete(file_field.name)

                # Update the file field to new path
                file_field.name = new_filename
                setattr(self, file_field_name, file_field)
