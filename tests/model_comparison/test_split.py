import pandas as pd
import pytest

from model_comparison.data import KEYS
from model_comparison.split import InteractionSplitter


def pairs(frame):
    return set(map(tuple, frame[KEYS].to_numpy()))


def test_splits_partition_the_input(toy_interactions):
    train, validation, test = InteractionSplitter().split(toy_interactions)
    assert pairs(train) | pairs(validation) | pairs(test) == pairs(toy_interactions)
    assert not (pairs(train) & pairs(validation))
    assert not (pairs(train) & pairs(test))
    assert not (pairs(validation) & pairs(test))
    assert len(train) + len(validation) + len(test) == len(toy_interactions)


def test_ten_interactions_split_6_2_2_and_short_history_stays_in_train(toy_interactions):
    train, validation, test = InteractionSplitter().split(toy_interactions)
    user_1 = lambda frame: (frame["user_id"] == 1).sum()
    assert (user_1(train), user_1(validation), user_1(test)) == (6, 2, 2)
    assert (train["user_id"] == 2).sum() == 2  # only two interactions: all train
    assert not (validation["user_id"] == 2).any() and not (test["user_id"] == 2).any()


def test_same_seed_is_repeatable_even_if_row_order_changes(toy_interactions):
    first = InteractionSplitter(seed=7).split(toy_interactions)
    shuffled = toy_interactions.sample(frac=1, random_state=3)
    second = InteractionSplitter(seed=7).split(shuffled)
    for a, b in zip(first, second):
        pd.testing.assert_frame_equal(a, b)


def test_different_seed_changes_the_split(toy_interactions):
    a = InteractionSplitter(seed=1).split(toy_interactions)[2]
    b = InteractionSplitter(seed=2).split(toy_interactions)[2]
    assert pairs(a) != pairs(b)


def test_rejects_duplicates_empty_and_bad_fractions(toy_interactions):
    with pytest.raises(ValueError):
        InteractionSplitter().split(pd.concat([toy_interactions, toy_interactions]))
    with pytest.raises(ValueError):
        InteractionSplitter().split(toy_interactions.iloc[0:0])
    with pytest.raises(ValueError):
        InteractionSplitter(validation_fraction=0.6, test_fraction=0.4)
