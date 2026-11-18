import hashlib


def pincode_enabled(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def make_pin(gid: str) -> str:
    """Return a stable four-digit PIN for a download GID."""
    digits = "".join(char for char in gid if char.isdigit())
    if len(digits) >= 4:
        return digits[:4]
    extra = "".join(
        char
        for char in hashlib.sha256(gid.encode()).hexdigest()
        if char.isdigit()
    )
    return f"{digits}{extra}0000"[:4]
