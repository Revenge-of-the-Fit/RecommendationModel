"""Adapter lookup. Adapters are named after the GitHub username that owns the model.

Adapter modules import model code only inside recommend().
"""
import importlib


def module_name(name: str) -> str:
    """Python module for an adapter name; a username's hyphen becomes an underscore."""
    return f"model_comparison.adapters.{name.replace('-', '_')}"


def get_adapter(name: str):
    return importlib.import_module(module_name(name))
