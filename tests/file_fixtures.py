import pytest


@pytest.fixture()
def file_fixtures():
    f = None

    def opener(filename):
        nonlocal f
        assert f is None
        f = open(filename)
        return f
    yield opener
    if f is not None:
        f.close()
