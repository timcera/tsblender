import os
import shutil
from io import StringIO
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_series_equal

from tsblender import tsblender


@pytest.fixture(scope="session")
def tmppath(tmp_path_factory):
    print(dir(tmp_path_factory))
    tmp_file_path = tmp_path_factory.getbasetemp()
    yield tmp_file_path
    print("Removing tmp_path directory...")
    if tmp_file_path.exists():
        shutil.rmtree(tmp_file_path)


def test_hydrologic_indices_compare(tmppath):
    tpath = Path(tmppath) / "test"
    shutil.copytree(os.path.dirname(os.path.abspath(__file__)), tpath)
    os.chdir(tpath)
    tsblender.run("tsproc_hydrologic_indices.inp")
    with open("data_out__hydrologic_indices.txt") as f:
        text = "\n".join([line for line in f if line[:3] == "   "])

    tsblender_series = pd.read_csv(
        StringIO(text),
        index_col=0,
        parse_dates=True,
        sep=r"  \s+",
        header=None,
    )
    tsblender_series = tsblender_series.reset_index()
    tsblender_series["index"] = tsblender_series.iloc[:, 0].str.split(":").str[0]
    tsblender_series = tsblender_series.set_index("index")
    tsblender_series = tsblender_series[
        ~tsblender_series.index.duplicated(keep="first")
    ]
    print(tsblender_series)

    with open("tsproc_out/data_out__hydrologic_indices.txt") as f:
        text = "\n".join([line for line in f if line[:3] == "   "])

    tsproc_series = pd.read_csv(
        StringIO(text),
        index_col=0,
        parse_dates=True,
        sep=r"  \s+",
        header=None,
    )
    tsproc_series = tsproc_series.reset_index()
    tsproc_series["index"] = tsproc_series.iloc[:, 0].str.split(":").str[0]
    tsproc_series = tsproc_series.set_index("index")
    tsproc_series = tsproc_series[~tsproc_series.index.duplicated(keep="first")]
    joined = tsblender_series.merge(tsproc_series, on="index", how="inner")

    tsblender_series = joined["1_x"].astype(float)
    tsproc_series = joined["1_y"].astype(float)

    assert_series_equal(
        tsblender_series, tsproc_series, atol=0.001, rtol=0.05, check_names=False
    )
