from datetime import datetime

ALLOWED_STATUSES = {"NEW", "VALIDATED", "ARCHIVED", "ERROR", "READY", "REJECTED"}

ALLOWED_TRANSITIONS = {
    "READY": {"VALIDATED", "ERROR"},
    "VALIDATED": {"ARCHIVED", "ERROR"},
    "ERROR": {"VALIDATED"},
    "ARCHIVED": set(),
}


def validate_status(status: str) -> bool:
    return status.upper() in ALLOWED_STATUSES


def validate_transition(old_status: str, new_status: str):
    allowed = ALLOWED_TRANSITIONS.get(old_status, set())

    if new_status not in allowed:
        return {
            "valid": False,
            "allowed": list(allowed),
        }

    return {"valid": True}


def build_history_record(invoice_id, old_status, new_status):
    return {
        "invoice_id": invoice_id,
        "changed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "old_status": old_status,
        "new_status": new_status,
    }