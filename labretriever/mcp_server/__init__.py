"""
Labretriever MCP server package.

Provides two entry points:

- ``labretriever-mcp`` / ``vdb_main``: VirtualDB query tools.
- ``labretriever-mcp-repo`` / ``repo_main``: DataCard scaffold and audit tools.

"""

from ._repo_server import main as repo_main
from ._vdb_server import main as vdb_main

__all__ = ["vdb_main", "repo_main"]
