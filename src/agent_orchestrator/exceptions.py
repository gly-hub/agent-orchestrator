"""Workflow engine exceptions."""

from __future__ import annotations


class WorkflowError(Exception):
    """Base exception for workflow failures."""


class WaitingForUser(WorkflowError):
    """Raised internally when a human node checkpoints a run."""

    def __init__(self, pending_action_id: str):
        super().__init__(f"waiting for user action: {pending_action_id}")
        self.pending_action_id = pending_action_id


class RegistryError(WorkflowError):
    """Raised when an agent or tool cannot be resolved."""


class StateResolutionError(WorkflowError):
    """Raised when template or path resolution fails."""


class WorkflowConfigError(WorkflowError):
    """Raised when a workflow configuration is invalid."""


class PermissionDenied(WorkflowError):
    """Raised when a tool call is rejected by policy."""
