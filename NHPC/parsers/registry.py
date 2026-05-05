from __future__ import annotations
from typing import Type
from .base import BaseLayout

_REGISTRY: list[BaseLayout] = []


def register(cls: Type[BaseLayout]) -> Type[BaseLayout]:
    """
    Class decorator that registers a layout variant.

    Registration order = detection priority.  Register specific layouts
    before generic ones.  FlatLayout must be registered last (it is the
    fallback and always returns True from detect()).

    Usage::

        from parsers.registry import register

        @register
        class MyLayout(BaseLayout):
            ...
    """
    _REGISTRY.append(cls())
    return cls


def get_registry() -> list[BaseLayout]:
    return list(_REGISTRY)
