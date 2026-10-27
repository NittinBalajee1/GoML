from pathlib import Path
import shutil
from uuid import uuid4
import pytest
from goml_pipeline.config import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    root = Path.cwd() / ".test-work" / str(uuid4())
    result = Settings(root=root)
    yield result
    shutil.rmtree(root, ignore_errors=True)
