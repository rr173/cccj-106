"""Error types for the local data-job orchestrator."""
from __future__ import annotations


class OrchestratorError(Exception):
    """Base class for every orchestrator-raised error."""


class DefinitionError(OrchestratorError):
    """The submitted job definition (or date list) is invalid.

    ``problems`` carries every problem found, so a rejected definition never
    starts half a graph and the operator sees the full list at once.
    """

    def __init__(self, problems: list[str]):
        self.problems = list(problems)
        super().__init__("invalid definition: " + "; ".join(self.problems))


class ControllerConflictError(OrchestratorError):
    """Another live controller already holds execution rights for the run."""


class RunStateError(OrchestratorError):
    """The requested operation is not allowed in the run's current state."""


class RunNotFoundError(OrchestratorError):
    """No orchestration record exists at the given run directory."""
