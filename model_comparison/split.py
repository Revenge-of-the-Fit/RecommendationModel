"""Seeded per-user random holdout, ported from Helixan's InteractionSplitter."""
import numpy as np
import pandas as pd

from model_comparison.data import KEYS


class InteractionSplitter:
    def __init__(self, validation_fraction: float = 0.2, test_fraction: float = 0.2, seed: int = 42):
        if not 0 < validation_fraction < 1 or not 0 < test_fraction < 1:
            raise ValueError("Validation and test fractions must be between 0 and 1")
        if validation_fraction + test_fraction >= 1:
            raise ValueError("Validation and test fractions must leave training data")
        if seed < 0:
            raise ValueError("The random seed cannot be negative")
        self.validation_fraction = validation_fraction
        self.test_fraction = test_fraction
        self.seed = seed

    def split(self, interactions: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        if interactions.empty:
            raise ValueError("Cannot split an empty interaction table")
        if interactions.duplicated(KEYS).any():
            raise ValueError("Split requires one row per user and movie")

        # Sorting first makes the split repeatable even if input row order changes
        ordered = interactions.sort_values(KEYS).reset_index(drop=True)
        random = np.random.default_rng(self.seed)
        training, validation, test = [], [], []

        for _, history in ordered.groupby("user_id", sort=True):
            count = len(history)
            if count < 3:
                training.extend(history.index)
                continue
            indices = random.permutation(history.index.to_numpy())
            validation_count = min(max(1, int(count * self.validation_fraction)), count - 2)
            test_count = min(max(1, int(count * self.test_fraction)), count - validation_count - 1)
            validation.extend(indices[:validation_count])
            test.extend(indices[validation_count:validation_count + test_count])
            training.extend(indices[validation_count + test_count:])

        def take(indices):
            return ordered.loc[sorted(indices)].reset_index(drop=True)

        return take(training), take(validation), take(test)
