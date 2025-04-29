import difflib
import os
import shutil
from pathlib import Path

from tsblender import tsblender


def test_files(tmp_path):
    target_dir = tmp_path / "hi_test_write_pest"
    shutil.copytree(Path(__file__).parent, target_dir)
    os.chdir(target_dir)
    tsblender.run("hi_test_write_pest.inp")

    for fname in [
        "BEC_HI.pst",
        "hi_list_output_obs.txt",
        "hi_list_output_sim.txt",
        "hi_list_sim_pest.txt",
        "simulated_vals.ins",
    ]:
        with open(fname) as file1:
            file1_info = file1.readlines()
        with open(str(Path("tsblender_reference") / fname)) as file2:
            file2_info = file2.readlines()

        diff = difflib.unified_diff(
            file1_info, file2_info, fromfile="file1.py", tofile="file2.py", lineterm=""
        )

        assert len(list(diff)) == 0
