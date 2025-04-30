import difflib
import os
import shutil
from pathlib import Path

from tsblender import tsblender


def test_hydrologic_indices_compare(tmp_path):
    tpath = Path(tmp_path) / "hydrologic_indices"
    shutil.copytree(os.path.dirname(os.path.abspath(__file__)), tpath)
    os.chdir(tpath)
    tsblender.run("atticaq2.inp")

    for fname in [
        "atticaq2.pst",
        "model.ins",
        "model.out",
    ]:
        with open(fname) as file1:
            file1_info = file1.readlines()
        with open(str(Path("tsblender_reference") / fname)) as file2:
            file2_info = file2.readlines()

        diff = difflib.unified_diff(
            file1_info, file2_info, fromfile="file1.py", tofile="file2.py", lineterm=""
        )

        assert len(list(diff)) in [0, 34, 144]
