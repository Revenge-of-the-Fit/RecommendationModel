import pytest

from model_comparison.adapters import get_adapter


@pytest.mark.parametrize("name", ["popularity", "Helixan", "MuhammadDF", "MajorTomLanded", "d-urbonas"])
def test_adapters_are_named_after_github_usernames(name):
    adapter = get_adapter(name)
    assert adapter.NAME == name
    assert adapter.REPO in (None, name)  # external/<username>
