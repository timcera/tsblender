import os
import unittest

import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from tsblender import tsblender


class TestSeriesCompare(unittest.TestCase):
    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        tsblender.run("tsproc_series_compare.inp")

    def test_series_compare(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        tsblender_series = pd.read_csv(
            "data_out__series_compare.txt",
            index_col=0,
            parse_dates=True,
            skiprows=9,
            sep=":",
            header=None,
        )

        tsproc_series = pd.read_csv(
            "tsproc_out/data_out__series_compare.txt",
            index_col=0,
            parse_dates=True,
            skiprows=9,
            sep=":",
            header=None,
        )

        assert_frame_equal(tsblender_series, tsproc_series, rtol=0.01, atol=0.01)

    def test_series_compare_values(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        tsblender_series = pd.read_csv(
            "data_out__series_compare_inputs.txt",
            parse_dates=[1],
            index_col=1,
            skiprows=4,
            header=None,
            sep=r"\s+",
            comment="T",
        )

        print(tsblender_series)
        print(tsblender_series.dtypes)
        tsblender_series.loc[:, 0] = tsblender_series.loc[:, 0].str.lower()
        tsblender_series = tsblender_series.reset_index()
        tsblender_series = tsblender_series.set_index([0, 1]).dropna(axis="index")

        tsproc_series = pd.read_csv(
            "tsproc_out/data_out__series_compare_inputs.txt",
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
        print(test_df)
        assert_series_equal(
            test_df["3_x"],
            test_df["3_y"],
            check_index=False,
            check_names=False,
            rtol=0.01,
            atol=0.01,
        )
