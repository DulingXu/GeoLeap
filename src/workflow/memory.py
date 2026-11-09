"""Shared accounting for cache-owned Python object graphs.

This is an accounted heap ledger, NOT a process RSS or native-memory cap.
It uses ``sys.getsizeof`` and object identity, includes its own control graph,
and leaves native/opaque internals and unregistered temporaries explicitly out
of scope. An OS memory limit must be reported separately.

Mutation lock order is budget transaction -> component lock. Root getters must
be read-only, must not acquire component locks, and should use weak references.
Bound-method getters are weak automatically; closures remain the caller's
responsibility. A rejected post-mutation check requires caller rollback.
"""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
import sys
import threading
import types
import weakref
from typing import Callable, Mapping


SCOPE = "cache-owned reachable Python allocations measured by sys.getsizeof; not process RSS"
EXCLUSIONS = (
    "native allocations not exposed by object.__sizeof__",
    "allocator arenas, fragmentation, interpreter and operating-system overhead",
    "callable, class, module, lock and weak-reference internals",
    "unregistered application objects and unregistered transient workspaces",
)
_OPAQUE = (types.FunctionType, types.BuiltinFunctionType, types.MethodType,
           types.BuiltinMethodType, types.ModuleType, type, weakref.ReferenceType,
           weakref.WeakMethod, type(threading.Lock()), type(threading.RLock()))
_LEAVES = (str, bytes, bytearray, int, float, complex, bool, type(None), range)


def _children(value):
    """Return declared Python references without following opaque internals."""
    if isinstance(value, _OPAQUE) or callable(value) or isinstance(value, _LEAVES):
        return (), isinstance(value, _OPAQUE) or callable(value)
    if isinstance(value, dict):
        return tuple(value.keys()) + tuple(value.values()), False
    if isinstance(value, (tuple, list, set, frozenset, deque)):
        return tuple(value), False
    if isinstance(value, memoryview):
        return (value.obj,), False
    children = []
    try:
        attributes = object.__getattribute__(value, "__dict__")
    except (AttributeError, TypeError):
        attributes = None
    if isinstance(attributes, dict):
        children.append(attributes)
    seen_slots = set()
    for cls in type(value).__mro__:
        slots = cls.__dict__.get("__slots__", ())
        if isinstance(slots, str):
            slots = (slots,)
        for name in slots:
            if name in {"__dict__", "__weakref__"}:
                continue
            if name.startswith("__") and not name.endswith("__"):
                name = "_" + cls.__name__.lstrip("_") + name
            if name in seen_slots:
                continue
            seen_slots.add(name)
            try:
                children.append(object.__getattribute__(value, name))
            except AttributeError:
                pass
    # Custom objects may hide native state even when __dict__ is present.
    return tuple(children), True


def measure_object_graph(roots: Mapping[str, object]) -> dict:
    """Pure identity-deduplicated graph accounting; root order owns sharing.

    The input mapping is a view, not an additional owned allocation. Each
    non-None root and reachable supported container is counted once across all
    roots. Equal, separately allocated buffers are charged separately. Opaque
    objects contribute their shallow ``getsizeof`` only, with scope disclosed.
    """
    if not isinstance(roots, Mapping):
        raise TypeError("roots must be a mapping")
    seen, categories, opaque_types = set(), {}, set()
    for name, root in roots.items():
        if not isinstance(name, str):
            raise TypeError("root names must be strings")
        subtotal, stack = 0, [] if root is None else [root]
        while stack:
            value = stack.pop()
            identity = id(value)
            if identity in seen:
                continue
            seen.add(identity)
            subtotal += sys.getsizeof(value)
            children, opaque = _children(value)
            if opaque:
                opaque_types.add(type(value).__module__ + "." + type(value).__qualname__)
            stack.extend(children)
        categories[name] = subtotal
    total = sum(categories.values())
    return {"owned_heap_bytes": total, "category_exclusive_bytes": categories,
            "unique_object_count": len(seen), "scope": SCOPE,
            "measurement": "sys.getsizeof with identity deduplication",
            "physical_budget_enforced": False, "complete_physical_budget": False,
            "opaque_types": sorted(opaque_types), "exclusions": list(EXCLUSIONS)}


class CacheMemoryError(MemoryError):
    """The declared accounted heap domain cannot fit the proposed mutation."""


@dataclass(frozen=True)
class _Root:
    category: str
    getter: object
    weak: bool

    def value(self):
        getter = self.getter() if self.weak else self.getter
        return None if getter is None else getter()


class RootHandle:
    def __init__(self, budget, name, registration):
        self._budget = weakref.ref(budget)
        self.name = name
        self._registration = registration

    def unregister(self):
        budget = self._budget()
        return False if budget is None else budget.unregister_root(self)

    close = unregister

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.unregister()


class _Reservation:
    def __init__(self, budget, token):
        self._budget = weakref.ref(budget)
        self.token = token
        self.active = True

    def commit(self):
        """Transfer reservation to installed roots; caller rolls back on error."""
        budget = self._budget()
        if budget is None:
            return
        with budget._lock:
            if not self.active:
                return
            budget._reservations.pop(self.token, None)
            self.active = False
            budget._event("commit", "", 0)
            budget.check()

    def _release(self, *, check=True):
        budget = self._budget()
        if budget is None:
            return
        with budget._lock:
            if not self.active:
                return
            budget._reservations.pop(self.token, None)
            self.active = False
            budget._event("release", "", 0)
            if check:
                budget.check()

    def release(self, *, check=True):
        self._release(check=check)


class CacheMemoryBudget:
    """One RLock-protected budget shared across runtime/store components.

    A small fixed control reserve (4 KiB + 512 bytes per journal slot) is charged
    in addition to the *actual* ledger object graph. It provides conservative
    headroom for scalar journal/control updates; it is not allocated payload.
    Reservations must be obtained before payload allocation. Roots, workspaces
    and retired leases must stay registered while the cache still owns them.
    """

    def __init__(self, limit_bytes, journal_limit=128):
        if isinstance(limit_bytes, bool) or not isinstance(limit_bytes, int) or limit_bytes < 0:
            raise ValueError("limit_bytes must be a nonnegative integer")
        if isinstance(journal_limit, bool) or not isinstance(journal_limit, int) or not 0 <= journal_limit <= 65536:
            raise ValueError("journal_limit must be an integer in [0, 65536]")
        self.limit_bytes = limit_bytes
        self.journal_limit = journal_limit
        self._lock = threading.RLock()
        self._roots = {}
        self._reservations = {}
        self._journal = [None] * journal_limit
        self._journal_position = 0
        self._sequence = 0
        self._next_token = 0
        self._peak_accounted_bytes = 0
        self._control_reserve_bytes = 4096 + journal_limit * 512
        self.check()

    def _event(self, action, category, size):
        self._sequence += 1
        if self.journal_limit:
            # Fixed-width scalar tuples only: no payload, exception or owner.
            self._journal[self._journal_position] = (self._sequence, action, category[:128], int(size))
            self._journal_position = (self._journal_position + 1) % self.journal_limit

    def _owned_roots_locked(self):
        # Clone/snapshot owners are short-lived. Drop only dead WeakMethod
        # registrations, not a live getter which legitimately returns None.
        dead = tuple(name for name, root in self._roots.items()
                     if root.weak and root.getter() is None)
        for name in dead:
            self._roots.pop(name, None)
        return {name: root.value() for name, root in self._roots.items()}

    def owned_roots(self):
        """Read-only root view for independent inspection; do not retain it."""
        with self._lock:
            return self._owned_roots_locked()

    def _snapshot_locked(self):
        values = {"__ledger__": self}
        values.update(self._owned_roots_locked())
        measured = measure_object_graph(values)
        by_category = {}
        for name, amount in measured["category_exclusive_bytes"].items():
            category = "__ledger__" if name == "__ledger__" else self._roots[name].category
            by_category[category] = by_category.get(category, 0) + amount
        operation_reserved = sum(size for size, _ in self._reservations.values())
        reserved = operation_reserved + self._control_reserve_bytes
        total = measured["owned_heap_bytes"] + reserved
        self._peak_accounted_bytes = max(self._peak_accounted_bytes, total)
        return {**measured, "category_exclusive_bytes": by_category,
                "root_exclusive_bytes": measured["category_exclusive_bytes"],
                "reserved_bytes": reserved, "operation_reserved_bytes": operation_reserved,
                "control_reserve_bytes": self._control_reserve_bytes,
                "total_accounted_bytes": total, "peak_accounted_bytes": self._peak_accounted_bytes,
                "limit_bytes": self.limit_bytes, "budget_limit_bytes": self.limit_bytes,
                "available_bytes": self.limit_bytes - total, "sequence": self._sequence,
                "budget_domain_id": id(self), "reclaimable_bytes": None,
                "reclaimable_scope": "Requires owner/lease-aware release analysis; not inferred from shallow record sizes.",
                "journal_limit": self.journal_limit,
                "journal": [entry for entry in sorted((item for item in self._journal if item is not None))]}

    def memory_snapshot(self):
        with self._lock:
            return self._snapshot_locked()

    def check(self):
        with self._lock:
            snapshot = self._snapshot_locked()
            if snapshot["total_accounted_bytes"] > self.limit_bytes:
                raise CacheMemoryError("accounted cache heap budget exceeded: "
                                       f"{snapshot['total_accounted_bytes']} > {self.limit_bytes}")
            return snapshot

    reconcile = check

    @contextmanager
    def transaction(self):
        """Hold the global mutation lock and check normal exit; never swallow."""
        with self._lock:
            yield self
            self.check()

    def register_root(self, name: str, getter: Callable, *, category=None):
        if (not isinstance(name, str) or not name or
                name in {"__ledger__", "__new_registration__"} or len(name) > 256):
            raise ValueError("root name must be a nonempty string of at most 256 characters")
        if not callable(getter):
            raise TypeError("root getter must be callable")
        category = name if category is None else category
        if not isinstance(category, str) or not category or len(category) > 128:
            raise ValueError("category must be a nonempty string of at most 128 characters")
        try:
            reference = weakref.WeakMethod(getter)
            weak = True
        except TypeError:
            reference, weak = getter, False
        with self._lock:
            if name in self._roots:
                raise ValueError("root name is already registered")
            registration = _Root(category, reference, weak)
            # Check the incoming graph before transferring it to ledger roots.
            # Include registration metadata, not only the payload estimate.
            prospective = {"__ledger__": self, **self._owned_roots_locked(),
                           name: registration.value(), "__new_registration__": (name, registration)}
            required = (measure_object_graph(prospective)["owned_heap_bytes"]
                        + sum(size for size, _ in self._reservations.values())
                        + self._control_reserve_bytes + 1024)
            if required > self.limit_bytes:
                raise CacheMemoryError("insufficient accounted cache heap for root registration")
            self._roots[name] = registration
            try:
                self._event("register", category, 0)
                self.check()
            except BaseException:
                self._roots.pop(name, None)
                raise
        return RootHandle(self, name, registration)

    def unregister_root(self, root):
        name = root.name if isinstance(root, RootHandle) else root
        with self._lock:
            if isinstance(root, RootHandle) and self._roots.get(name) is not root._registration:
                return False
            removed = self._roots.pop(name, None)
            if removed is not None:
                self._event("unregister", removed.category, 0)
            self.check()
            return removed is not None

    def acquire_reservation(self, size_bytes, category="workspace") -> _Reservation:
        """Atomically reserve capacity and return with the global lock released.

        Use this for a request lifetime spanning remote service execution.
        The token stays charged until ``commit`` or ``release``; the caller
        must release it in ``finally`` on failure. This does not protect root
        mutation: install/rollback owned roots inside ``transaction`` before
        committing. Do not hold a transaction across remote service calls.
        """
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
            raise ValueError("reservation size must be a nonnegative integer")
        if not isinstance(category, str) or not category or len(category) > 128:
            raise ValueError("reservation category must be a nonempty string of at most 128 characters")
        with self._lock:
            # Reject before creating the payload or publishing a reservation.
            current = self.check()
            if current["total_accounted_bytes"] + size_bytes + 1024 > self.limit_bytes:
                raise CacheMemoryError("insufficient accounted cache heap for reservation")
            self._next_token += 1
            token = self._next_token
            self._reservations[token] = (size_bytes, category)
            self._event("reserve", category, size_bytes)
            try:
                self.check()
            except BaseException:
                self._reservations.pop(token, None)
                raise
            return _Reservation(self, token)

    @contextmanager
    def reservation(self, size_bytes, category="workspace"):
        """Reserve and retain the global lock over a local critical section.

        For long-lived request/telemetry reservations use
        ``acquire_reservation`` instead, then commit/release explicitly.
        """
        with self._lock:
            reservation = self.acquire_reservation(size_bytes, category)
            try:
                yield reservation
            finally:
                # Preserve the caller's original exception; rollback belongs to
                # that caller, and a second check must not mask its failure.
                reservation._release(check=sys.exc_info()[0] is None)
