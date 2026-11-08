"""Operation context for provider adapters and request-scoped accounting.

Core execution reports purpose and identity; actual token receipts stay with
the provider adapter. Context is local to the current thread/async context.
"""

from contextlib import contextmanager
from contextvars import ContextVar

_operation_context = ContextVar("geoleap_operation_context", default=None)


def current_operation_context() -> dict:
    return dict(_operation_context.get() or {})


@contextmanager
def operation_scope(**fields):
    context = current_operation_context()
    context.update({key: value for key, value in fields.items() if value is not None})
    token = _operation_context.set(context)
    try:
        yield dict(context)
    finally:
        _operation_context.reset(token)
