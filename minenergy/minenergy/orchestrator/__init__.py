"""Local data-job orchestrator for bounded offline date sets.

Public API::

    from minenergy.orchestrator import (
        load_definition, validate_dates, plan_lines,
        create_run, execute_run, start_run, cancel_run, rerun_job, run_status,
    )

See ``minenergy.orchestrator.controller`` for the execution entry points and
``minenergy.orchestrator.store`` for the record layer.  The orchestrator is a
local, finite-run tool: no daemons, no schedules, no web UI.
"""
from __future__ import annotations

from .controller import (
    EXIT_CANCELLED,
    EXIT_CONFLICT,
    EXIT_DEFINITION,
    EXIT_FAILED,
    EXIT_INTERRUPTED,
    EXIT_OK,
    Controller,
    cancel_run,
    create_run,
    execute_run,
    rerun_job,
    run_status,
    start_run,
)
from .definition import (
    Definition,
    JobInstance,
    Limits,
    NodeDef,
    definition_from_dict,
    expand,
    load_definition,
    plan_lines,
    validate_dates,
)
from .errors import (
    ControllerConflictError,
    DefinitionError,
    OrchestratorError,
    RunNotFoundError,
    RunStateError,
)
from .store import RunStore

__all__ = [
    "Controller",
    "ControllerConflictError",
    "Definition",
    "DefinitionError",
    "EXIT_CANCELLED",
    "EXIT_CONFLICT",
    "EXIT_DEFINITION",
    "EXIT_FAILED",
    "EXIT_INTERRUPTED",
    "EXIT_OK",
    "JobInstance",
    "Limits",
    "NodeDef",
    "OrchestratorError",
    "RunNotFoundError",
    "RunStateError",
    "RunStore",
    "cancel_run",
    "create_run",
    "definition_from_dict",
    "execute_run",
    "expand",
    "load_definition",
    "plan_lines",
    "rerun_job",
    "run_status",
    "start_run",
    "validate_dates",
]
