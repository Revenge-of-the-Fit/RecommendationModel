from model_comparison.repos import resolve_repo


def test_helixan_uses_the_local_src_when_this_repo_contains_the_model(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "recommender.py").write_text("")
    assert resolve_repo("helixan", tmp_path / "external", project_root=tmp_path) == tmp_path


def test_helixan_falls_back_to_the_external_clone_otherwise(tmp_path):
    result = resolve_repo("helixan", tmp_path / "external", project_root=tmp_path)
    assert result == tmp_path / "external" / "helixan"


def test_other_models_always_come_from_external(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "recommender.py").write_text("")
    assert resolve_repo("muhammad", tmp_path / "external", project_root=tmp_path) == (
        tmp_path / "external" / "muhammad"
    )


def test_in_harness_adapters_have_no_repo(tmp_path):
    assert resolve_repo(None, tmp_path / "external", project_root=tmp_path) is None
