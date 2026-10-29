# Importing the package installs the network guard, so it applies under both
# `python -m pytest` and `python -m unittest` (running a test file directly skips it).
import sys
from pathlib import Path

# Several test modules import each other by bare name (for example `import test_live_users`)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from tests import network_guard

network_guard.install()
