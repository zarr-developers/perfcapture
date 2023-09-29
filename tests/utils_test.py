from perfcapture import utils
import pathlib

def test_path_not_empty(tmp_path: pathlib.Path):
    """Test utils.path_not_empty.
    
    Args:
        tmp_path: See https://docs.pytest.org/en/7.4.x/how-to/tmp_path.html
    """
    # Create an empty temp directory
    d = tmp_path / "sub"
    d.mkdir()
    assert not utils.path_not_empty(d)
    
    # Put a file into the temp directory
    filename = d / "hello.txt"
    filename.write_text("TEST", encoding="utf-8")
    assert utils.path_not_empty(d)
    
    # Add another file
    filename2 = d / ".foo"
    filename2.write_text("TEST", encoding="utf-8")
    assert utils.path_not_empty(d)