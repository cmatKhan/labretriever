"""Data fetchers for HuggingFace Hub integration."""

import logging
import re
from typing import Any

import requests
from huggingface_hub import DatasetCard, repo_info
from requests import HTTPError

from labretriever.constants import get_hf_token
from labretriever.errors import HfDataFetchError


class HfDataCardFetcher:
    """Handles fetching dataset cards from HuggingFace Hub."""

    def __init__(self, token: str | None = None):
        """
        Initialize the fetcher.

        :param token: HuggingFace token for authentication

        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.token = token or get_hf_token()

    def fetch(self, repo_id: str, repo_type: str = "dataset") -> dict[str, Any]:
        """
        Fetch and return dataset card data.

        :param repo_id: Repository identifier (e.g., "user/dataset")
        :param repo_type: Type of repository ("dataset", "model", "space")
        :return: Dataset card data as dictionary
        :raises HfDataFetchError: If fetching fails

        """
        try:
            self.logger.debug(f"Fetching dataset card for {repo_id}")
            card = DatasetCard.load(repo_id, repo_type=repo_type, token=self.token)

            if not card.data:
                self.logger.warning(f"Dataset card for {repo_id} has no data section")
                return {}

            return card.data.to_dict()

        except Exception as e:
            error_msg = f"Failed to fetch dataset card for {repo_id}: {e}"
            self.logger.error(error_msg)
            raise HfDataFetchError(error_msg) from e


class HfSizeInfoFetcher:
    """Handles fetching size information from HuggingFace Dataset Server API."""

    def __init__(self, token: str | None = None):
        """
        Initialize the fetcher.

        :param token: HuggingFace token for authentication

        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.token = token or get_hf_token()
        self.base_url = "https://datasets-server.huggingface.co"

    def _build_headers(self) -> dict[str, str]:
        """Build request headers with authentication if available."""
        headers = {"User-Agent": "TFBP-API/1.0"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def fetch(self, repo_id: str) -> dict[str, Any]:
        """
        Fetch dataset size information.

        :param repo_id: Repository identifier (e.g., "user/dataset")
        :return: Size information as dictionary
        :raises HfDataFetchError: If fetching fails

        """
        url = f"{self.base_url}/size"
        params = {"dataset": repo_id}
        headers = self._build_headers()

        try:
            self.logger.debug(f"Fetching size info for {repo_id}")
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()
            self.logger.debug(f"Size info fetched successfully for {repo_id}")
            return data

        except HTTPError as e:
            if e.response.status_code == 404:
                error_msg = f"Dataset {repo_id} not found"
            elif e.response.status_code == 403:
                error_msg = (
                    f"Access denied to dataset {repo_id} (check token permissions)"
                )
            else:
                error_msg = f"HTTP error fetching size for {repo_id}: {e}"

            self.logger.error(error_msg)
            raise HfDataFetchError(error_msg) from e

        except requests.RequestException as e:
            error_msg = f"Request failed fetching size for {repo_id}: {e}"
            self.logger.error(error_msg)
            raise HfDataFetchError(error_msg) from e

        except ValueError as e:
            error_msg = f"Invalid JSON response fetching size for {repo_id}: {e}"
            self.logger.error(error_msg)
            raise HfDataFetchError(error_msg) from e


class HfRepoStructureFetcher:
    """Handles fetching repository structure from HuggingFace Hub."""

    def __init__(self, token: str | None = None):
        """
        Initialize the fetcher.

        :param token: HuggingFace token for authentication

        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.token = token or get_hf_token()
        self._cached_structure: dict[str, dict[str, Any]] = {}

    def fetch(self, repo_id: str, force_refresh: bool = False) -> dict[str, Any]:
        """
        Fetch repository structure information.

        :param repo_id: Repository identifier (e.g., "user/dataset")
        :param force_refresh: If True, bypass cache and fetch fresh data
        :return: Repository structure information
        :raises HfDataFetchError: If fetching fails

        """
        # Check cache first unless force refresh is requested
        if not force_refresh and repo_id in self._cached_structure:
            self.logger.debug(f"Using cached repo structure for {repo_id}")
            return self._cached_structure[repo_id]

        try:
            self.logger.debug(f"Fetching repo structure for {repo_id}")
            info = repo_info(repo_id=repo_id, repo_type="dataset", token=self.token)

            # Extract file structure
            files = []
            partitions: dict[str, set] = {}

            for sibling in info.siblings or []:
                file_info = {
                    "path": sibling.rfilename,
                    "size": sibling.size,
                    "is_lfs": sibling.lfs is not None,
                }
                files.append(file_info)

                # Extract partition information from file paths
                self._extract_partition_info(sibling.rfilename, partitions)

            result = {
                "repo_id": repo_id,
                "files": files,
                "partitions": partitions,
                "total_files": len(files),
                "last_modified": (
                    info.last_modified.isoformat() if info.last_modified else None
                ),
            }

            # Cache the result
            self._cached_structure[repo_id] = result
            return result

        except Exception as e:
            error_msg = f"Failed to fetch repo structure for {repo_id}: {e}"
            self.logger.error(error_msg)
            raise HfDataFetchError(error_msg) from e

    def _extract_partition_info(
        self, file_path: str, partitions: dict[str, set[str]]
    ) -> None:
        """
        Extract partition information from file paths.

        :param file_path: Path to analyze for partitions
        :param partitions: Dictionary to update with partition info

        """
        # Look for partition patterns like "column=value" in path
        partition_pattern = r"([^/=]+)=([^/]+)"
        matches = re.findall(partition_pattern, file_path)

        for column, value in matches:
            if column not in partitions:
                partitions[column] = set()
            partitions[column].add(value)

    def get_partition_values(
        self, repo_id: str, partition_column: str, force_refresh: bool = False
    ) -> list[str]:
        """
        Get all values for a specific partition column.

        :param repo_id: Repository identifier
        :param partition_column: Name of the partition column
        :param force_refresh: If True, bypass cache and fetch fresh data
        :return: List of unique partition values
        :raises HfDataFetchError: If fetching fails

        """
        structure = self.fetch(repo_id, force_refresh=force_refresh)
        partition_values = structure.get("partitions", {}).get(partition_column, set())
        return sorted(list(partition_values))

    def get_dataset_files(
        self, repo_id: str, path_pattern: str | None = None, force_refresh: bool = False
    ) -> list[dict[str, Any]]:
        """
        Get dataset files, optionally filtered by path pattern.

        :param repo_id: Repository identifier
        :param path_pattern: Optional regex pattern to filter files
        :param force_refresh: If True, bypass cache and fetch fresh data
        :return: List of matching files
        :raises HfDataFetchError: If fetching fails

        """
        structure = self.fetch(repo_id, force_refresh=force_refresh)
        files = structure["files"]

        if path_pattern:
            pattern = re.compile(path_pattern)
            files = [f for f in files if pattern.search(f["path"])]

        return files
