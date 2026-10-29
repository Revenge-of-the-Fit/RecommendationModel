# Fail any test that tries to reach a host outside this machine (see network_guard.py).
# pytest loads this file automatically; plain `python -m unittest` and running a test
# file directly do not, so pytest is the supported runner.
import network_guard

network_guard.install()
