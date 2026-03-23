"""Tests for datainfo fetcher classes."""

from unittest.mock import Mock, patch

import pytest
import requests
from requests import HTTPError

from labretriever.errors import HfDataFetchError
from labretriever.fetchers import (
    HfDataCardFetcher,
    HfRepoStructureFetcher,
    HfSizeInfoFetcher,
)


class TestHfDataCardFetcher:
    """Test HfDataCardFetcher class."""

    def test_init_with_token(self, test_token):
        """Test initialization with token."""
        fetcher = HfDataCardFetcher(token=test_token)
        assert fetcher.token == test_token

    def test_init_without_token(self):
        """Test initialization without token."""
        with patch.dict("os.environ", {}, clear=True):
            fetcher = HfDataCardFetcher()
            assert fetcher.token is None

    def test_init_with_env_token(self, test_token):
        """Test initialization with environment token."""
        with patch.dict("os.environ", {"HF_TOKEN": test_token}):
            fetcher = HfDataCardFetcher()
            assert fetcher.token == test_token

    @patch("labretriever.fetchers.DatasetCard")
    def test_fetch_success(
        self, mock_dataset_card, test_repo_id, sample_dataset_card_data
    ):
        """Test successful dataset card fetch."""
        # Setup mock
        mock_card = Mock()
        mock_card.data.to_dict.return_value = sample_dataset_card_data
        mock_dataset_card.load.return_value = mock_card

        fetcher = HfDataCardFetcher(token="test_token")
        result = fetcher.fetch(test_repo_id)

        assert result == sample_dataset_card_data
        mock_dataset_card.load.assert_called_once_with(
            test_repo_id, repo_type="dataset", token="test_token"
        )

    @patch("labretriever.fetchers.DatasetCard")
    def test_fetch_no_data_section(self, mock_dataset_card, test_repo_id):
        """Test fetch when dataset card has no data section."""
        # Setup mock with no data
        mock_card = Mock()
        mock_card.data = None
        mock_dataset_card.load.return_value = mock_card

        fetcher = HfDataCardFetcher()
        result = fetcher.fetch(test_repo_id)

        assert result == {}

    @patch("labretriever.fetchers.DatasetCard")
    def test_fetch_exception(self, mock_dataset_card, test_repo_id):
        """Test fetch when DatasetCard.load raises exception."""
        mock_dataset_card.load.side_effect = Exception("API Error")

        fetcher = HfDataCardFetcher()

        with pytest.raises(HfDataFetchError, match="Failed to fetch dataset card"):
            fetcher.fetch(test_repo_id)

    def test_fetch_different_repo_types(self, sample_dataset_card_data):
        """Test fetch with different repository types."""
        with patch("labretriever.fetchers.DatasetCard") as mock_dataset_card:
            mock_card = Mock()
            mock_card.data.to_dict.return_value = sample_dataset_card_data
            mock_dataset_card.load.return_value = mock_card

            fetcher = HfDataCardFetcher()

            # Test with model repo
            fetcher.fetch("test/repo", repo_type="model")
            mock_dataset_card.load.assert_called_with(
                "test/repo", repo_type="model", token=None
            )

            # Test with space repo
            fetcher.fetch("test/repo", repo_type="space")
            mock_dataset_card.load.assert_called_with(
                "test/repo", repo_type="space", token=None
            )


class TestHfSizeInfoFetcher:
    """Test HfSizeInfoFetcher class."""

    def test_init(self, test_token):
        """Test initialization."""
        fetcher = HfSizeInfoFetcher(token=test_token)
        assert fetcher.token == test_token
        assert fetcher.base_url == "https://datasets-server.huggingface.co"

    def test_build_headers_with_token(self, test_token):
        """Test building headers with token."""
        fetcher = HfSizeInfoFetcher(token=test_token)
        headers = fetcher._build_headers()

        assert headers["User-Agent"] == "TFBP-API/1.0"
        assert headers["Authorization"] == f"Bearer {test_token}"

    def test_build_headers_without_token(self):
        """Test building headers without token."""
        fetcher = HfSizeInfoFetcher()
        headers = fetcher._build_headers()

        assert headers["User-Agent"] == "TFBP-API/1.0"
        assert "Authorization" not in headers

    @patch("labretriever.fetchers.requests.get")
    def test_fetch_success(self, mock_get, test_repo_id, sample_size_info):
        """Test successful size info fetch."""
        # Setup mock response
        mock_response = Mock()
        mock_response.json.return_value = sample_size_info
        mock_get.return_value = mock_response

        fetcher = HfSizeInfoFetcher(token="test_token")
        result = fetcher.fetch(test_repo_id)

        assert result == sample_size_info
        mock_get.assert_called_once()

        # Check call arguments
        call_args = mock_get.call_args
        assert call_args[1]["params"]["dataset"] == test_repo_id
        assert call_args[1]["headers"]["Authorization"] == "Bearer test_token"
        assert call_args[1]["timeout"] == 30

    @patch("labretriever.fetchers.requests.get")
    def test_fetch_404_error(self, mock_get, test_repo_id):
        """Test fetch with 404 error."""
        # Setup mock 404 response
        mock_response = Mock()
        mock_response.status_code = 404
        error = HTTPError(response=mock_response)
        mock_get.side_effect = error

        fetcher = HfSizeInfoFetcher()

        with pytest.raises(HfDataFetchError, match="Dataset .* not found"):
            fetcher.fetch(test_repo_id)

    @patch("labretriever.fetchers.requests.get")
    def test_fetch_403_error(self, mock_get, test_repo_id):
        """Test fetch with 403 error."""
        # Setup mock 403 response
        mock_response = Mock()
        mock_response.status_code = 403
        error = HTTPError(response=mock_response)
        mock_get.side_effect = error

        fetcher = HfSizeInfoFetcher()

        with pytest.raises(
            HfDataFetchError, match="Access denied.*check token permissions"
        ):
            fetcher.fetch(test_repo_id)

    @patch("labretriever.fetchers.requests.get")
    def test_fetch_other_http_error(self, mock_get, test_repo_id):
        """Test fetch with other HTTP error."""
        # Setup mock 500 response
        mock_response = Mock()
        mock_response.status_code = 500
        error = HTTPError(response=mock_response)
        mock_get.side_effect = error

        fetcher = HfSizeInfoFetcher()

        with pytest.raises(HfDataFetchError, match="HTTP error fetching size"):
            fetcher.fetch(test_repo_id)

    @patch("labretriever.fetchers.requests.get")
    def test_fetch_request_exception(self, mock_get, test_repo_id):
        """Test fetch with request exception."""
        mock_get.side_effect = requests.RequestException("Network error")

        fetcher = HfSizeInfoFetcher()

        with pytest.raises(HfDataFetchError, match="Request failed fetching size"):
            fetcher.fetch(test_repo_id)

    @patch("labretriever.fetchers.requests.get")
    def test_fetch_json_decode_error(self, mock_get, test_repo_id):
        """Test fetch with JSON decode error."""
        # Setup mock response with invalid JSON
        mock_response = Mock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response

        fetcher = HfSizeInfoFetcher()

        with pytest.raises(HfDataFetchError, match="Invalid JSON response"):
            fetcher.fetch(test_repo_id)


class TestHfRepoStructureFetcher:
    """Test HfRepoStructureFetcher class."""

    def test_init(self, test_token):
        """Test initialization."""
        fetcher = HfRepoStructureFetcher(token=test_token)
        assert fetcher.token == test_token
        assert fetcher._cached_structure == {}

    @patch("labretriever.fetchers.repo_info")
    def test_fetch_success(self, mock_repo_info, test_repo_id, sample_repo_structure):
        """Test successful repository structure fetch."""
        # Setup mock repo info
        mock_info = Mock()
        mock_info.siblings = [
            Mock(rfilename="features.parquet", size=2048000, lfs=Mock()),
            Mock(rfilename="binding/part1.parquet", size=1024000, lfs=Mock()),
            Mock(
                rfilename="tracks/regulator=TF1/experiment=exp1/data.parquet",
                size=5120000,
                lfs=Mock(),
            ),
        ]
        mock_info.last_modified.isoformat.return_value = "2023-12-01T10:30:00Z"
        mock_repo_info.return_value = mock_info

        fetcher = HfRepoStructureFetcher(token="test_token")
        result = fetcher.fetch(test_repo_id)

        assert result["repo_id"] == test_repo_id
        assert result["total_files"] == 3
        assert len(result["files"]) == 3
        assert result["last_modified"] == "2023-12-01T10:30:00Z"

        # Check that repo_info was called correctly
        mock_repo_info.assert_called_once_with(
            repo_id=test_repo_id, repo_type="dataset", token="test_token"
        )

    @patch("labretriever.fetchers.repo_info")
    def test_fetch_with_caching(self, mock_repo_info, test_repo_id):
        """Test fetch with caching behavior."""
        # Setup mock
        mock_info = Mock()
        mock_info.siblings = []
        mock_info.last_modified = None
        mock_repo_info.return_value = mock_info

        fetcher = HfRepoStructureFetcher()

        # First fetch
        result1 = fetcher.fetch(test_repo_id)
        assert mock_repo_info.call_count == 1

        # Second fetch should use cache
        result2 = fetcher.fetch(test_repo_id)
        assert mock_repo_info.call_count == 1  # Not called again
        assert result1 == result2

        # Force refresh should call API again
        fetcher.fetch(test_repo_id, force_refresh=True)
        assert mock_repo_info.call_count == 2

    @patch("labretriever.fetchers.repo_info")
    def test_fetch_siblings_none(self, mock_repo_info, test_repo_id):
        """Test fetch when siblings is None."""
        # Setup mock with None siblings
        mock_info = Mock()
        mock_info.siblings = None
        mock_info.last_modified = None
        mock_repo_info.return_value = mock_info

        fetcher = HfRepoStructureFetcher()
        result = fetcher.fetch(test_repo_id)

        assert result["total_files"] == 0
        assert result["files"] == []
        assert result["partitions"] == {}

    @patch("labretriever.fetchers.repo_info")
    def test_fetch_exception(self, mock_repo_info, test_repo_id):
        """Test fetch when repo_info raises exception."""
        mock_repo_info.side_effect = Exception("API Error")

        fetcher = HfRepoStructureFetcher()

        with pytest.raises(HfDataFetchError, match="Failed to fetch repo structure"):
            fetcher.fetch(test_repo_id)

    def test_extract_partition_info(self):
        """Test extracting partition information from file paths."""
        fetcher = HfRepoStructureFetcher()
        partitions = {}  # type: ignore

        # Test normal partition pattern
        fetcher._extract_partition_info(
            "data/regulator=TF1/condition=control/file.parquet", partitions
        )
        assert "regulator" in partitions
        assert "TF1" in partitions["regulator"]
        assert "condition" in partitions
        assert "control" in partitions["condition"]

        # Test multiple values for same partition
        fetcher._extract_partition_info(
            "data/regulator=TF2/condition=treatment/file.parquet", partitions
        )
        assert len(partitions["regulator"]) == 2
        assert "TF2" in partitions["regulator"]
        assert "treatment" in partitions["condition"]

        # Test file without partitions
        fetcher._extract_partition_info("simple_file.parquet", partitions)
        # partitions dict should remain unchanged
        assert len(partitions) == 2

    @patch("labretriever.fetchers.repo_info")
    def test_get_partition_values_success(self, mock_repo_info, test_repo_id):
        """Test getting partition values for a specific column."""
        # Setup mock with partitioned files
        mock_info = Mock()
        mock_info.siblings = [
            Mock(rfilename="data/regulator=TF1/file1.parquet", size=1000, lfs=None),
            Mock(rfilename="data/regulator=TF2/file2.parquet", size=1000, lfs=None),
            Mock(rfilename="data/regulator=TF3/file3.parquet", size=1000, lfs=None),
        ]
        mock_info.last_modified = None
        mock_repo_info.return_value = mock_info

        fetcher = HfRepoStructureFetcher()
        values = fetcher.get_partition_values(test_repo_id, "regulator")

        assert values == ["TF1", "TF2", "TF3"]  # Should be sorted

    @patch("labretriever.fetchers.repo_info")
    def test_get_partition_values_no_partitions(self, mock_repo_info, test_repo_id):
        """Test getting partition values when no partitions exist."""
        # Setup mock with no partitioned files
        mock_info = Mock()
        mock_info.siblings = [
            Mock(rfilename="simple_file.parquet", size=1000, lfs=None),
        ]
        mock_info.last_modified = None
        mock_repo_info.return_value = mock_info

        fetcher = HfRepoStructureFetcher()
        values = fetcher.get_partition_values(test_repo_id, "regulator")

        assert values == []

    @patch("labretriever.fetchers.repo_info")
    def test_get_dataset_files_all(self, mock_repo_info, test_repo_id):
        """Test getting all dataset files."""
        # Setup mock
        mock_info = Mock()
        mock_info.siblings = [
            Mock(rfilename="file1.parquet", size=1000, lfs=None),
            Mock(rfilename="file2.parquet", size=2000, lfs=Mock()),
        ]
        mock_info.last_modified = None
        mock_repo_info.return_value = mock_info

        fetcher = HfRepoStructureFetcher()
        files = fetcher.get_dataset_files(test_repo_id)

        assert len(files) == 2
        assert files[0]["path"] == "file1.parquet"
        assert files[0]["size"] == 1000
        assert files[0]["is_lfs"] is False

        assert files[1]["path"] == "file2.parquet"
        assert files[1]["size"] == 2000
        assert files[1]["is_lfs"] is True

    @patch("labretriever.fetchers.repo_info")
    def test_get_dataset_files_with_pattern(self, mock_repo_info, test_repo_id):
        """Test getting dataset files with path pattern filter."""
        # Setup mock
        mock_info = Mock()
        mock_info.siblings = [
            Mock(rfilename="data/file1.parquet", size=1000, lfs=None),
            Mock(rfilename="metadata/info.json", size=500, lfs=None),
            Mock(rfilename="data/file2.parquet", size=2000, lfs=None),
        ]
        mock_info.last_modified = None
        mock_repo_info.return_value = mock_info

        fetcher = HfRepoStructureFetcher()
        files = fetcher.get_dataset_files(test_repo_id, path_pattern=r".*\.parquet$")

        assert len(files) == 2
        assert all(f["path"].endswith(".parquet") for f in files)

    def test_get_dataset_files_uses_cache(self):
        """Test that get_dataset_files uses fetch caching."""
        fetcher = HfRepoStructureFetcher()

        with patch.object(fetcher, "fetch") as mock_fetch:
            mock_fetch.return_value = {"files": []}

            # First call
            fetcher.get_dataset_files("test/repo")
            mock_fetch.assert_called_with("test/repo", force_refresh=False)

            # Second call with force_refresh
            fetcher.get_dataset_files("test/repo", force_refresh=True)
            mock_fetch.assert_called_with("test/repo", force_refresh=True)

    def test_get_partition_values_uses_cache(self):
        """Test that get_partition_values uses fetch caching."""
        fetcher = HfRepoStructureFetcher()

        with patch.object(fetcher, "fetch") as mock_fetch:
            mock_fetch.return_value = {"partitions": {"regulator": {"TF1", "TF2"}}}

            # First call
            result = fetcher.get_partition_values("test/repo", "regulator")
            mock_fetch.assert_called_with("test/repo", force_refresh=False)
            assert result == ["TF1", "TF2"]

            # Second call with force_refresh
            fetcher.get_partition_values("test/repo", "regulator", force_refresh=True)
            mock_fetch.assert_called_with("test/repo", force_refresh=True)
