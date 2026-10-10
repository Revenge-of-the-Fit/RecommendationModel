import json

from model_comparison import compare
from model_comparison.job import JobResult
from model_comparison.report import render_markdown
from test_compare import args, patched, synthetic_tables  # noqa: F401  (patched is a fixture)

COSTS = {
    "fit_seconds": 1.0, "fit_memory_growth_bytes": 10, "peak_rss_bytes": 20, "model_size_bytes": 30,
    "model_size_note": "pickle of the trained model", "requests": 3, "latency_p50_ms": 1.0,
    "latency_p95_ms": 2.0, "latency_max_ms": 3.0, "throughput_per_s": 500.0,
}


class CostRunner:
    """Gives every job a distinct fit time (1, 2, 3, ...) so the test can tell which job was kept."""

    def __init__(self):
        self.jobs = []

    def __call__(self, job):
        self.jobs.append(job)
        top = job.interactions["movie_id"].value_counts().index.tolist()
        costs = {**COSTS, "fit_seconds": float(len(self.jobs))}
        return JobResult({int(u): top[: job.k + 3] for u in job.user_ids}, notes={"costs": costs, "other": 1})


def test_payload_keeps_the_costs_of_the_final_job_not_of_tuning(patched):
    runner = CostRunner()
    payload = compare.run_comparison(args(models=["Helixan"]), runner=runner)
    assert payload["models"]["Helixan"]["costs"]["fit_seconds"] == float(len(runner.jobs))
    assert payload["models"]["Helixan"]["notes"] == {"other": 1}       # costs moved out of the notes
    assert payload["machine"]["cpu_count"] >= 1


def test_only_costs_skips_tuning_and_scoring(patched, tmp_path):
    runner = CostRunner()
    payload = compare.run_comparison(
        args(models=["popularity", "Helixan"], only_costs=True, cost_users=3, results_dir=tmp_path), runner=runner
    )
    train_validation = {len(job.interactions) for job in runner.jobs}
    assert len(runner.jobs) == 2                      # one final job per model, no tuning grid
    assert len(train_validation) == 1
    for result in payload["models"].values():
        assert result["costs"]["requests"] == 3
        assert result["shared_sample"] is None and result["full_population"] is None
        assert len(runner.jobs[0].user_ids) == 3      # cost_users bounds the timed users


def test_only_costs_reuses_params_selected_by_a_previous_comparison(patched, tmp_path):
    (tmp_path / "comparison.json").write_text(
        json.dumps({"models": {"Helixan": {"selected_params": {"regularization": 500, "min_rating": 8}}}})
    )
    runner = CostRunner()
    compare.run_comparison(args(models=["Helixan"], only_costs=True, results_dir=tmp_path), runner=runner)
    assert runner.jobs[0].params == {"regularization": 500, "min_rating": 8}


def test_params_for_costs_falls_back_to_the_middle_of_the_grid():
    adapter = compare.get_adapter("MuhammadDF")
    assert compare.params_for_costs(adapter, None) == {"n_neighbors": 20}
    assert compare.params_for_costs(adapter, {"n_neighbors": 40}) == {"n_neighbors": 40}
    assert compare.params_for_costs(adapter, {"bogus": 1}) == {"n_neighbors": 20}  # not in the grid


def test_repeats_report_the_median_fit_time_and_keep_other_costs_from_the_first_run(patched):
    runner = CostRunner()
    payload = compare.run_comparison(
        args(models=["popularity"], only_costs=True, cost_repeats=3), runner=runner
    )
    costs = payload["models"]["popularity"]["costs"]
    assert len(runner.jobs) == 3
    assert costs["fit_seconds_runs"] == [1.0, 2.0, 3.0]
    assert costs["fit_seconds"] == 2.0


def test_sampled_models_are_not_repeated(patched):
    runner = CostRunner()
    compare.run_comparison(args(models=["d-urbonas"], only_costs=True, cost_repeats=3), runner=runner)
    assert len(runner.jobs) == 1                      # repeating would repeat paid API calls


def test_report_has_a_cost_table_and_marks_unmeasured_values(tmp_path):
    payload = {
        "config": {"seed": 1, "top_k": 10, "relevance_rating": 7, "only_costs": True, "cost_users": 5,
                   "cost_repeats": 1},
        "split": {"training": 1, "validation": 1, "test": 1, "test_users_evaluable": 1, "sample_users": 1,
                  "test_users_without_description": 0},
        "machine": {"platform": "TestOS", "processor": "arm", "cpu_count": 8, "memory_bytes": 16 * 1024**3,
                    "python": "3.13"},
        "models": {
            "alpha": {"costs": COSTS, "selected_params": {"n_neighbors": 20}, "failed_users": {}, "drops": {},
                      "notes": {}},
            "beta": {"costs": {**COSTS, "fit_seconds": None, "fit_memory_growth_bytes": None,
                               "model_size_bytes": None, "latency_p95_ms": None},
                     "selected_params": {}, "failed_users": {}, "drops": {}, "notes": {}},
        },
    }
    text = render_markdown(payload)
    assert "Cost" in text and "TestOS" in text and "16.0 GiB" in text
    assert "| alpha | 1.00 s |" in text
    assert "n/a" in text                                # beta has no fit and no size
    assert "- **alpha**: n_neighbors=20" in text        # costs depend on the hyperparameters measured
    assert "NDCG" not in text                           # an only-costs report has no accuracy tables


def test_parser_accepts_the_cost_options():
    parsed = compare.build_parser().parse_args(["--only-costs", "--cost-users", "50", "--cost-repeats", "3"])
    assert parsed.only_costs is True and parsed.cost_users == 50 and parsed.cost_repeats == 3
    defaults = compare.build_parser().parse_args([])
    assert defaults.only_costs is False and defaults.cost_users == 200 and defaults.cost_repeats == 1
