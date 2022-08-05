import runpy
import shutil
import sys
from pathlib import Path

import pytest


@pytest.fixture
def script_folder() -> Path:
    return Path(__file__).parent.parent / "scripts"


@pytest.mark.integrationtest
def test_modify_xml(tmpdir, script_folder):
    # Copy XML files
    f = Path(__file__).parent / "data/micro_t1_nl/20171002T0930Z_1D_NL_SSH_3.xml"
    tmp_file = tmpdir / "ex.xml"
    shutil.copyfile(f, tmp_file)

    script = script_folder / "modify_xml.py"
    sys.argv[1:] = [str(tmpdir) + "/*"]
    runpy.run_path(str(script))
