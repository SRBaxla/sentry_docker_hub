import pytest

@pytest.fixture(scope="session")
def any_config():
    return {"mock_mode": True}
