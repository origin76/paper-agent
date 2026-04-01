from __future__ import annotations

from .narrative_stack import narrative as _impl


for _name, _value in vars(_impl).items():
    if _name.startswith("__"):
        continue
    globals()[_name] = _value

__all__ = [
    name
    for name in globals()
    if not name.startswith("__") and name not in {"_impl", "_name", "_value"}
]

del _impl
del _name
del _value


if __name__ == "__main__":
    from .narrative_stack.narrative import main

    raise SystemExit(main())
