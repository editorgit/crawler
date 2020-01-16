
@pytest.fixture
def test_parser():
    assert openfile(name.txt).read() == "Content A"
