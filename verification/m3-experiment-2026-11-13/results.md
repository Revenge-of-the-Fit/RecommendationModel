# Experiment: tracking-smoke-20261113

| Measure | Control | Treatment |
| --- | ---: | ---: |
| assigned_users | 4002 | 3973 |
| observed_units | 3993 | 3954 |
| fee_coverage | 0.772101 | 0.771371 |
| mean_spend_per_assigned_user_usd | unknown | unknown |
| mean_fee_per_observed_unit_usd | unknown | unknown |
| rating_users | 15 | 12 |
| rated_units | 15 | 12 |
| rating_user_coverage | 0.00374813 | 0.00302039 |
| mean_user_rating | 7.33333 | 7.25 |

Differences are treatment minus control.

| Comparison | Difference | 95% CI | p-value | Status |
| --- | ---: | --- | ---: | --- |
| spend_per_assigned_user_usd | unknown | unknown | unknown | unknown_missing_fees |
| spend_per_user_with_complete_fees_usd | 0.00378217 | -0.00346149 to 0.0110258 | 0.30608 | estimated |
| mean_rating_per_rater | -0.0833333 | -1.08836 to 0.921694 | 0.865776 | estimated |
| mean_fee_per_observed_unit_usd | unknown | unknown | unknown | descriptive |

Assignment-based comparison, including failed requests, fallback responses and users with no watches. Ratings describe raters only; missing fees and ratings are unknown. Complete-fee users are a conditional secondary comparison. Tests assume independent users; 95% intervals are approximate and per metric, not joint. No sequential stopping on significance.

See results.json for fee/rating coverage, capture diagnostics, assignment balance and the fixed plan.
