
@pytest.yield_fixture
def open_file():
    f = None

    def opener(filename):
        nonlocal f
        assert f is None
        f = open(filename)
        return f
    yield opener
    if f is not None:
        f.close()
