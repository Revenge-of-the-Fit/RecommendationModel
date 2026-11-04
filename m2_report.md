## Offline evaluation

We checked the recommender by hiding some recorded movie history, training on the rest, and seeing whether it recommended movies the users had liked.

**Metric:** We used NDCG@10, a score from 0 to 1 for the top ten recommendations. Higher is better. A hidden movie counts as relevant when the user rated it at least 7. Finding it near the top earns more credit.

Each match adds `1 / log2(position + 1)` to the user's score. We divide by the best possible total for that user's relevant movies, up to ten, then average across users. Missed movies earn no credit.

**Data:** We used the saved October 28 `movielog2` export: 13,619 events, 5,000 users, and 2,000 movies. The selected events span 02:32:40–05:09:30 UTC on October 28.
This is a selected subset of the requested 24 hours. We checked that the copied files matched the saved export.

**Operationalization (how we ran it):** We combined each person's watches and latest rating for one movie into one sample. Keeping these records together prevents the same sample from entering both training and testing.

We reused the existing split: sort samples by user/movie, then shuffle each user's samples with one NumPy `default_rng(42)`. For users with at least three samples, reserve two groups of 20%, rounding down but keeping at least one in each. The second group is the test set. We combine the first group with the remaining training data because the model settings are already fixed. Users with fewer than three samples stay in training.

We trained a fresh EASE model once on 8,101 samples, keeping 152 samples for testing. Production settings stayed at regularization 100 and minimum positive rating 6. We learned weights, user profiles, and popularity only from training data. Recommendations exclude movies already in the user's training history. Ties use training popularity, then movie ID. Relevant test movies the model cannot recommend still count toward the score.

[Run instructions](OFFLINE_EVALUATION.md#run), [evaluation command](src/evaluate.py),
[split and scoring](src/evaluation.py), and [data preparation](src/dataset.py).

**Result:** On November 3, 2026, NDCG@10 was 0.170877 across 45 users.
Of the 5,000 users, 4,848 had too little history for a test sample. Of the remaining 152, another 107 had no hidden movie rated at least 7, leaving 45 we could score. Each had one highly rated hidden movie: it appeared in the top ten for 16 users, and was missing for 29. Its position determined how much NDCG credit it earned.

**Limits:** Only 45 users qualified, so this is a small sample. The export favors active users and movies selected using the previous model's popularity, capped at 200,000 source records, 5,000 users, and 2,000 movies. Users and movies repeat across the training and test groups. This check measures existing-user preferences; future traffic and the LLM cold-start path need separate evaluation.
