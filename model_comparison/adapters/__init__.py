"""Adapter lookup. Adapter modules import model code only inside recommend()."""
import importlib


def get_adapter(name: str):
    return importlib.import_module(f"model_comparison.adapters.{name}")
