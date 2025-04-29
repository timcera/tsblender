import difflib
import os
import shutil
from pathlib import Path

from tsblender import tsblender


def test_files(tmp_path):
    target_dir = tmp_path / "black_creek_flow_duration"
    shutil.copytree(Path(__file__).parent, target_dir)
    os.chdir(target_dir)
    tsblender.run("hi_test_inv_indices.inp")

    for fname in [
        "hi_list_output_series.txt",
        "hi_list_output.txt",
    ]:
        with open(fname) as file1:
            file1_info = file1.readlines()
        with open(str(Path("tsblender_reference") / fname)) as file2:
            file2_info = file2.readlines()

        diff = difflib.unified_diff(
            file1_info, file2_info, fromfile="file1.py", tofile="file2.py", lineterm=""
        )

        assert len(list(diff)) == 0
