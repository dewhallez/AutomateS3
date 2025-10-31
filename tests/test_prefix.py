from pathlib import Path
import pytest

from s3_uploader import determine_prefix


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("script.py", "python"),
        ("SCRIPT.PY", "python"),
        ("photo.jpg", "pictures"),
        ("photo.JPEG", "pictures"),
        ("image.png", "pictures"),
        ("notes.txt", None),
        ("archive.tar.gz", None),
    ],
)
def test_determine_prefix(filename, expected):
    p = Path(filename)
    assert determine_prefix(p) == expected
