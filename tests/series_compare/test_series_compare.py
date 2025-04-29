import os
import shutil
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from tsblender import tsblender


def test_series_compare(tmp_path):
    tpath = Path(tmp_path) / "series_compare"
    shutil.copytree(
        os.path.dirname(os.path.abspath(__file__)), tpath, dirs_exist_ok=True
    )
    os.chdir(tpath)
    tsblender.run("tsproc_series_compare.inp")

    tsblender_series = pd.read_csv(
        "data_out__series_compare.txt",
        index_col=0,
        parse_dates=True,
        skiprows=9,
        sep=":",
        header=None,
    )

    tsproc_series = pd.read_csv(
        "tsproc_reference/data_out__series_compare.txt",
        index_col=0,
        parse_dates=True,
        skiprows=9,
        sep=":",
        header=None,
    )

    assert_frame_equal(tsblender_series, tsproc_series, rtol=0.01, atol=0.01)


def test_series_compare_values(tmp_path):
    tpath = Path(tmp_path) / "series_compare"
    shutil.copytree(
        os.path.dirname(os.path.abspath(__file__)), tpath, dirs_exist_ok=True
    )
    os.chdir(tpath)
    tsblender.run("tsproc_series_compare.inp")

    tsblender_series = pd.read_csv(
        "data_out__series_compare_inputs.txt",
        parse_dates=[1],
        index_col=1,
        skiprows=4,
        header=None,
        sep=r"\s+",
        comment="T",
    )

    tsblender_series.loc[:, 0] = tsblender_series.loc[:, 0].str.lower()
    tsblender_series = tsblender_series.reset_index()
    tsblender_series = tsblender_series.set_index([0, 1]).dropna(axis="index")

    tsproc_series = pd.read_csv(
        "tsproc_reference/data_out__series_compare_inputs.txt",
        parse_dates=[1],
        index_col=1,
        skiprows=3,
        header=None,
        sep=r"\s+",
        comment="T",
    )

    tsproc_series.loc[:, 0] = tsproc_series.loc[:, 0].str.lower()
    tsproc_series = tsproc_series.reset_index()
    tsproc_series = tsproc_series.set_index([0, 1]).dropna(axis="index")

    test_df = tsblender_series.merge(
        tsproc_series, left_index=True, right_index=True, how="right"
    )
    assert_series_equal(
        test_df["3_x"],
        test_df["3_y"],
        check_index=False,
        check_names=False,
        rtol=0.01,
        atol=0.01,
    )
