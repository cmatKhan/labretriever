import hashlib
import json
import subprocess
import uuid
from pathlib import Path

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models

from .service import Service


def unique_upload_to(instance, filename):
    """
    Generate a unique upload path by appending a UUID to the filename.
    """
    model_name = instance.__class__.__name__.lower()
    # Extract file extension
    file_ext = Path(filename).suffix
    # generate a unique name using UUID
    unique_name = f"{uuid.uuid4()}{file_ext}"
    return f"{model_name}/{unique_name}"


class FileModel(models.Model):
    """
    Mixin to provide file, md5sum, and clean logic for models.
    Dynamically sets upload_to based on the instance's class name.
    """

    file = models.FileField(
        upload_to=unique_upload_to,  # Use the dynamic callable
        help_text="FileField representing the file",
        blank=True,
        null=True,
        unique=True,
    )
    md5sum = models.CharField(
        max_length=32,
        blank=True,
        help_text="CharField representing the MD5 checksum of the file",
    )
    fileformat = models.ForeignKey(
        "FileFormat",
        on_delete=models.CASCADE,
        related_name=f"{__name__.lower()}",
        help_text=(
            "ForeignKey to the FileFormat model, representing the format "
            "of the FASTA file"
        ),
    )
    tool = models.ForeignKey(Service, null=True, blank=True, on_delete=models.PROTECT)
    tool_params = models.JSONField(default=dict, blank=True)
    inputs = models.ManyToManyField(
        "self",
        symmetrical=False,
        related_name="outputs",
        blank=True,
    )
    status = models.CharField(
        max_length=20,
        choices=[("ready", "Ready"), ("pending", "Pending"), ("failed", "Failed")],
        default="ready",
    )

    class Meta:
        abstract = True

    def clean(self):
        super().clean()
        if self.file and not self.md5sum:
            raise ValidationError(
                {
                    "md5sum": "MD5 checksum must be provided if a file is uploaded.",
                },
            )

    def hash(self):
        """Create a deterministic hash of the command and inputs."""
        input_paths = [inp.pull() for inp in self.inputs.all()]
        key_data = {
            "tool": self.tool.name if self.tool else None,
            "params": self.tool_params,
            "inputs": sorted(str(p) for p in input_paths),
        }
        return hashlib.sha256(json.dumps(key_data, sort_keys=True).encode()).hexdigest()

    def materialize_path(self):
        """Return a filesystem path to the output tarball."""
        outdir = Path(settings.MEDIA_ROOT) / "genomemap_generated"
        outdir.mkdir(parents=True, exist_ok=True)
        return outdir / f"{self.hash()}.tar.gz"

    def pull(self):
        """Get the materialized file, or generate it."""
        if self.file:
            return Path(self.file.path)

        cached_path = self.materialize_path()
        if cached_path.exists():
            return cached_path

        # Trigger async job (placeholder, you can hook in Celery/Slurm/etc.)
        self.status = "pending"
        self.save()

        try:
            self._run_tool(cached_path)
            self.file.name = str(cached_path.relative_to(settings.MEDIA_ROOT))
            self.status = "ready"
            self.save()
            return cached_path
        except Exception as e:
            self.status = "failed"
            self.save()
            raise RuntimeError(f"Tool execution failed: {e}")

    def _run_tool(self, output_path):
        if not self.tool:
            raise ValidationError("No tool is defined for this GenomeMap.")

        input_paths = [inp.pull() for inp in self.inputs.all()]

        cmd = self.tool.command_template.format(
            params=" ".join(
                f"{k} {v}" if not isinstance(v, bool) else k
                for k, v in self.tool_params.items()
            ),
        )

        cmd = cmd.replace("{input}", " ".join(str(p) for p in input_paths))
        cmd = cmd + f" > {output_path}"

        result = subprocess.run(cmd, check=False, shell=True, capture_output=True)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.decode())
