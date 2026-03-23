import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import duckdb
from huggingface_hub import scan_cache_dir, try_to_load_from_cache
from huggingface_hub.utils import DeleteCacheStrategy

from labretriever.datacard import DataCard


class HfCacheManager(DataCard):
    """Enhanced cache management for Hugging Face Hub with metadata-focused
    retrieval."""

    def __init__(
        self,
        repo_id: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
        token: str | None = None,
        logger: logging.Logger | None = None,
    ):
        super().__init__(repo_id, token)
        self.duckdb_conn = duckdb_conn
        self.logger = logger or logging.getLogger(__name__)

    def _get_metadata_for_config(
        self, config, force_refresh: bool = False
    ) -> dict[str, Any]:
        """
        Get metadata for a specific configuration using 3-case strategy.

        :param config: Configuration object to process
        :param force_refresh: If True, skip cache checks and download fresh from remote

        """
        config_result = {
            "config_name": config.config_name,
            "strategy": None,
            "table_name": None,
            "success": False,
            "message": "",
        }

        table_name = f"metadata_{config.config_name}"

        try:
            # Skip cache checks if force_refresh is True
            if not force_refresh:
                # Case 1: Check if metadata already exists in DuckDB
                if self._check_metadata_exists_in_duckdb(table_name):
                    config_result.update(
                        {
                            "strategy": "duckdb_exists",
                            "table_name": table_name,
                            "success": True,
                            "message": f"Metadata table {table_name} "
                            "already exists in DuckDB",
                        }
                    )
                    return config_result

                # Case 2: Check if HF data is in cache, create DuckDB representation
                if self._load_metadata_from_cache(config, table_name):
                    config_result.update(
                        {
                            "strategy": "cache_loaded",
                            "table_name": table_name,
                            "success": True,
                            "message": "Loaded metadata from cache "
                            f"into table {table_name}",
                        }
                    )
                    return config_result

            # Case 3: Download from HF (explicit vs embedded)
            if self._download_and_load_metadata(config, table_name):
                config_result.update(
                    {
                        "strategy": "downloaded",
                        "table_name": table_name,
                        "success": True,
                        "message": "Downloaded and loaded metadata "
                        f"into table {table_name}",
                    }
                )
                return config_result

            config_result["message"] = (
                f"Failed to retrieve metadata for {config.config_name}"
            )

        except Exception as e:
            config_result["message"] = f"Error processing {config.config_name}: {e}"
            self.logger.error(f"Error in metadata config {config.config_name}: {e}")

        return config_result

    def _check_metadata_exists_in_duckdb(self, table_name: str) -> bool:
        """Case 1: Check if metadata table already exists in DuckDB database."""
        try:
            # Query information schema to check if table exists
            result = self.duckdb_conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()

            exists = result is not None
            if exists:
                self.logger.debug(f"Table {table_name} already exists in DuckDB")
            return exists

        except Exception as e:
            self.logger.debug(f"Error checking DuckDB table existence: {e}")
            return False

    def _load_metadata_from_cache(self, config, table_name: str) -> bool:
        """Case 2: HF data in cache, create DuckDB representation."""
        try:
            # Check if metadata files are cached locally
            cached_files = []
            for data_file in config.data_files:
                cached_path = try_to_load_from_cache(
                    repo_id=self.repo_id,
                    filename=data_file.path,
                    repo_type="dataset",
                )

                if isinstance(cached_path, str) and Path(cached_path).exists():
                    cached_files.append(cached_path)

            if not cached_files:
                self.logger.debug(f"No cached files found for {config.config_name}")
                return False

            # Load cached parquet files into DuckDB
            self._create_duckdb_table_from_files(
                cached_files, table_name, config.config_name
            )
            self.logger.info(
                f"Loaded {len(cached_files)} cached files into {table_name}"
            )
            return True

        except Exception as e:
            self.logger.debug(f"Error loading from cache for {config.config_name}: {e}")
            return False

    def _download_and_load_metadata(self, config, table_name: str) -> bool:
        """Case 3: Download from HF (explicit vs embedded)."""
        try:
            from huggingface_hub import snapshot_download

            # Download specific files for this metadata config
            file_patterns = [data_file.path for data_file in config.data_files]

            downloaded_path = snapshot_download(
                repo_id=self.repo_id,
                repo_type="dataset",
                allow_patterns=file_patterns,
                token=self.token,
            )

            # Find downloaded parquet files
            downloaded_files = []
            for pattern in file_patterns:
                file_path = Path(downloaded_path) / pattern
                if file_path.exists() and file_path.suffix == ".parquet":
                    downloaded_files.append(str(file_path))
                else:
                    # Handle wildcard patterns, including nested wildcards
                    if "*" in pattern:
                        # Use glob on the full pattern relative to downloaded_path
                        base_path = Path(downloaded_path)
                        matching_files = list(base_path.glob(pattern))
                        downloaded_files.extend(
                            [str(f) for f in matching_files if f.suffix == ".parquet"]
                        )
                    else:
                        # Handle non-wildcard patterns that might be directories
                        parent_dir = Path(downloaded_path) / Path(pattern).parent
                        if parent_dir.exists():
                            downloaded_files.extend(
                                [str(f) for f in parent_dir.glob("*.parquet")]
                            )

            if not downloaded_files:
                self.logger.warning(
                    f"No parquet files found after download for {config.config_name}"
                )
                return False

            # Load downloaded files into DuckDB
            self._create_duckdb_table_from_files(
                downloaded_files, table_name, config.config_name
            )
            self.logger.info(
                f"Downloaded and loaded {len(downloaded_files)} files into {table_name}"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"Error downloading metadata for {config.config_name}: {e}"
            )
            return False

    def _create_duckdb_table_from_files(
        self, file_paths: list[str], table_name: str, config_name: str
    ) -> None:
        """Create DuckDB table/view from parquet files."""
        if len(file_paths) == 1:
            # Single file
            create_sql = f"""
            CREATE OR REPLACE VIEW {table_name} AS
            SELECT * FROM read_parquet('{file_paths[0]}')
            """
        else:
            # Multiple files
            files_str = "', '".join(file_paths)
            create_sql = f"""
            CREATE OR REPLACE VIEW {table_name} AS
            SELECT * FROM read_parquet(['{files_str}'])
            """

        self.duckdb_conn.execute(create_sql)
        self.logger.debug(
            f"Created DuckDB view {table_name} from {len(file_paths)} files"
        )

        # Validate source_sample fields if they exist
        self._validate_source_sample_fields(table_name, config_name)

    def _validate_source_sample_fields(self, table_name: str, config_name: str) -> None:
        """
        Validate source_sample fields have correct format.

        Composite sample identifiers must be in the format:
        "repo_id;config_name;sample_id" (exactly 3 semicolon-separated parts)

        """
        config = self.get_config(config_name)

        # Find all source_sample fields
        source_sample_fields = [
            f.name
            for f in config.dataset_info.features  # type: ignore
            if f.role == "source_sample"
        ]

        if not source_sample_fields:
            return  # No validation needed

        # For each field, validate format
        for field_name in source_sample_fields:
            query = f"""
            SELECT {field_name},
                   LENGTH({field_name}) - LENGTH(REPLACE({field_name}, ';', ''))
                   AS semicolon_count
            FROM {table_name}
            WHERE semicolon_count != 2
            LIMIT 1
            """
            result = self.duckdb_conn.execute(query).fetchone()

            if result:
                raise ValueError(
                    f"Invalid format in field '{field_name}' "
                    f"with role='source_sample'. "
                    f"Expected 'repo_id;config_name;sample_id' "
                    f"(3 semicolon-separated parts), "
                    f"but found: '{result[0]}'"
                )

    def _extract_embedded_metadata_field(
        self, data_table_name: str, field_name: str, metadata_table_name: str
    ) -> bool:
        """Extract a specific metadata field from a data table."""
        try:
            # Create a metadata view with unique values from the specified field
            extract_sql = f"""
            CREATE OR REPLACE VIEW {metadata_table_name} AS
            SELECT DISTINCT {field_name} as value, COUNT(*) as count
            FROM {data_table_name}
            WHERE {field_name} IS NOT NULL
            GROUP BY {field_name}
            ORDER BY count DESC
            """

            self.duckdb_conn.execute(extract_sql)

            # Verify the table was created and has data
            count_result = self.duckdb_conn.execute(
                f"SELECT COUNT(*) FROM {metadata_table_name}"
            ).fetchone()

            if count_result and count_result[0] > 0:
                self.logger.info(
                    f"Extracted {count_result[0]} unique values for {field_name} "
                    f"into {metadata_table_name}"
                )
                return True
            else:
                self.logger.warning(f"No data found for field {field_name}")
                return False

        except Exception as e:
            self.logger.error(f"Error extracting field {field_name}: {e}")
            return False

    def clean_cache_by_age(
        self,
        max_age_days: int = 30,
        dry_run: bool = True,
    ) -> DeleteCacheStrategy:
        """
        Clean cache entries older than specified age.

        :param max_age_days: Remove revisions older than this many days
        :param  dry_run: If True, show what would be deleted without executing
            size_threshold: Only delete if total cache size exceeds this (e.g., "10GB")

        :return: DeleteCacheStrategy object that can be executed

        """
        cache_info = scan_cache_dir()
        cutoff_date = datetime.now() - timedelta(days=max_age_days)

        old_revisions = []
        for repo in cache_info.repos:
            for revision in repo.revisions:
                # Check if revision is older than cutoff
                revision_date = datetime.fromtimestamp(revision.last_modified)
                if revision_date < cutoff_date:
                    old_revisions.append(revision.commit_hash)
                    self.logger.debug(
                        f"Marking for deletion: {revision.commit_hash} "
                        f"(last modified: {revision.last_modified})"
                    )

        if not old_revisions:
            self.logger.info("No old revisions found to delete")
            # return None

        delete_strategy = cache_info.delete_revisions(*old_revisions)

        self.logger.info(
            f"Found {len(old_revisions)} old revisions. "
            f"Will free {delete_strategy.expected_freed_size_str}"
        )

        if not dry_run:
            delete_strategy.execute()
            self.logger.info(
                f"Cache cleanup completed. Freed "
                f"{delete_strategy.expected_freed_size_str}"
            )
        else:
            self.logger.info("Dry run completed. Use dry_run=False to execute deletion")

        return delete_strategy

    def clean_cache_by_size(
        self,
        target_size: str,
        strategy: Literal[
            "oldest_first", "largest_first", "least_used"
        ] = "oldest_first",
        dry_run: bool = True,
    ) -> DeleteCacheStrategy:
        """
        Clean cache to reach target size by removing revisions.

        :param target_size: Target cache size (e.g., "5GB", "500MB")
        :param strategy: Deletion strategy - "oldest_first", "largest_first",
            "least_used"
        :param dry_run: If True, show what would be deleted without executing

        :return: DeleteCacheStrategy object that can be executed

        """
        cache_info = scan_cache_dir()
        current_size = cache_info.size_on_disk
        target_bytes = self._parse_size_string(target_size)

        if current_size <= target_bytes:
            self.logger.info(
                f"Cache size ({cache_info.size_on_disk_str}) already below "
                f"target ({target_size})"
            )

        bytes_to_free = current_size - target_bytes

        # Get all revisions sorted by strategy
        all_revisions = []
        for repo in cache_info.repos:
            for revision in repo.revisions:
                all_revisions.append(revision)

        # Sort revisions based on strategy
        if strategy == "oldest_first":
            all_revisions.sort(key=lambda r: r.last_modified)
        elif strategy == "largest_first":
            all_revisions.sort(key=lambda r: r.size_on_disk, reverse=True)
        elif strategy == "least_used":
            # Use last_modified as proxy for usage
            all_revisions.sort(key=lambda r: r.last_modified)
        else:
            raise ValueError(f"Unknown strategy: {strategy}")

        # Select revisions to delete
        revisions_to_delete = []
        freed_bytes = 0

        for revision in all_revisions:
            if freed_bytes >= bytes_to_free:
                break
            revisions_to_delete.append(revision.commit_hash)
            freed_bytes += revision.size_on_disk

        if not revisions_to_delete:
            self.logger.warning("No revisions selected for deletion")

        delete_strategy = cache_info.delete_revisions(*revisions_to_delete)

        self.logger.info(
            f"Selected {len(revisions_to_delete)} revisions for deletion. "
            f"Will free {delete_strategy.expected_freed_size_str}"
        )

        if not dry_run:
            delete_strategy.execute()
            self.logger.info(
                f"Cache cleanup completed. Freed "
                f"{delete_strategy.expected_freed_size_str}"
            )
        else:
            self.logger.info("Dry run completed. Use dry_run=False to execute deletion")

        return delete_strategy

    def clean_unused_revisions(
        self, keep_latest: int = 2, dry_run: bool = True
    ) -> DeleteCacheStrategy:
        """
        Clean unused revisions, keeping only the latest N revisions per repo.

        :param keep_latest: Number of latest revisions to keep per repo
        :param dry_run: If True, show what would be deleted without executing
        :return: DeleteCacheStrategy object that can be executed

        """
        cache_info = scan_cache_dir()
        revisions_to_delete = []

        for repo in cache_info.repos:
            # Sort revisions by last modified (newest first)
            sorted_revisions = sorted(
                repo.revisions, key=lambda r: r.last_modified, reverse=True
            )

            # Keep the latest N, mark the rest for deletion
            if len(sorted_revisions) > keep_latest:
                old_revisions = sorted_revisions[keep_latest:]
                for revision in old_revisions:
                    revisions_to_delete.append(revision.commit_hash)
                    self.logger.debug(
                        f"Marking old revision for deletion: {repo.repo_id} - "
                        f"{revision.commit_hash}"
                    )

        delete_strategy = cache_info.delete_revisions(*revisions_to_delete)

        self.logger.info(
            f"Found {len(revisions_to_delete)} unused revisions. "
            f"Will free {delete_strategy.expected_freed_size_str}"
        )

        if not dry_run:
            delete_strategy.execute()
            self.logger.info(
                f"Cache cleanup completed. Freed "
                f"{delete_strategy.expected_freed_size_str}"
            )
        else:
            self.logger.info("Dry run completed. Use dry_run=False to execute deletion")

        return delete_strategy

    def auto_clean_cache(
        self,
        max_age_days: int = 30,
        max_total_size: str = "10GB",
        keep_latest_per_repo: int = 2,
        dry_run: bool = True,
    ) -> list[DeleteCacheStrategy]:
        """
        Automated cache cleaning with multiple strategies.

        :param max_age_days: Remove revisions older than this
        :param max_total_size: Target maximum cache size
        :param keep_latest_per_repo: Keep this many latest revisions per repo
        :param dry_run: If True, show what would be deleted without executing
        :return: List of DeleteCacheStrategy objects that were executed

        """
        strategies_executed = []

        self.logger.info("Starting automated cache cleanup...")

        # Step 1: Remove very old revisions
        strategy = self.clean_cache_by_age(max_age_days=max_age_days, dry_run=dry_run)
        if strategy:
            strategies_executed.append(strategy)

        # Step 2: Remove unused revisions (keep only latest per repo)
        strategy = self.clean_unused_revisions(
            keep_latest=keep_latest_per_repo, dry_run=dry_run
        )
        if strategy:
            strategies_executed.append(strategy)

        # Step 3: If still over size limit, remove more aggressively
        cache_info = scan_cache_dir()
        if cache_info.size_on_disk > self._parse_size_string(max_total_size):
            strategy = self.clean_cache_by_size(
                target_size=max_total_size, strategy="oldest_first", dry_run=dry_run
            )
            if strategy:
                strategies_executed.append(strategy)

        total_freed = sum(s.expected_freed_size for s in strategies_executed)
        self.logger.info(
            f"Automated cleanup complete. Total freed: "
            f"{self._format_bytes(total_freed)}"
        )

        return strategies_executed

    def _parse_size_string(self, size_str: str) -> int:
        """Parse size string like '10GB' to bytes."""
        size_str = size_str.upper().strip()

        # Check longer units first to avoid partial matches
        multipliers = {"TB": 1024**4, "GB": 1024**3, "MB": 1024**2, "KB": 1024, "B": 1}

        for unit, multiplier in multipliers.items():
            if size_str.endswith(unit):
                number = float(size_str[: -len(unit)])
                return int(number * multiplier)

        # If no unit specified, assume bytes
        return int(size_str)

    def _format_bytes(self, bytes_size: int) -> str:
        """Format bytes into human readable string."""
        if bytes_size == 0:
            return "0B"

        # iterate over common units, dividing by 1024 each time, to find an
        # appropriate unit. Default to TB if the size is very large
        size = float(bytes_size)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024.0:
                return f"{size:.1f}{unit}"
            size /= 1024.0
        return f"{size:.1f}TB"

    def query(self, sql: str, config_name: str, refresh_cache: bool = False) -> Any:
        """
        Execute SQL query against a specific dataset configuration.

        Loads the specified configuration and executes the SQL query.
        Automatically replaces the config name in the SQL with the actual
        table name for user convenience.

        :param sql: SQL query to execute
        :param config_name: Configuration name to query (table will be loaded
            if needed)
        :param refresh_cache: If True, force refresh from remote instead of
            using cache
        :return: DataFrame with query results
        :raises ValueError: If config_name not found or query fails

        Example:
            mgr = HfCacheManager("BrentLab/harbison_2004", duckdb.connect())
            df = mgr.query(
                "SELECT DISTINCT sample_id FROM harbison_2004",
                "harbison_2004"
            )

        """
        # Validate config exists
        if config_name not in [c.config_name for c in self.configs]:
            available_configs = [c.config_name for c in self.configs]
            raise ValueError(
                f"Config '{config_name}' not found. "
                f"Available configs: {available_configs}"
            )

        # Load the configuration data
        config = self.get_config(config_name)
        if not config:
            raise ValueError(f"Could not retrieve config '{config_name}'")

        config_result = self._get_metadata_for_config(
            config, force_refresh=refresh_cache
        )
        if not config_result.get("success", False):
            raise ValueError(
                f"Failed to load data for config '{config_name}': "
                f"{config_result.get('message', 'Unknown error')}"
            )

        table_name = config_result.get("table_name")
        if not table_name:
            raise ValueError(f"No table available for config '{config_name}'")

        # Replace config name with actual table name in SQL for user convenience
        modified_sql = sql.replace(config_name, table_name)

        # Execute query
        try:
            result = self.duckdb_conn.execute(modified_sql).fetchdf()
            self.logger.debug(f"Query executed successfully on {config_name}")
            return result
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            self.logger.error(f"SQL: {modified_sql}")
            raise ValueError(f"Query execution failed: {e}") from e
