import re


def aphiaid_from_lsid(input: str) -> int | None:
    pattern = r"urn:lsid:marinespecies\.org:taxname:(\d+)"
    match = re.match(pattern, input)

    if match:
        return int(match.group(1))
    else:
        return None


def try_float(value) -> float:
    try:
        return float(value)
    except Exception:
        return None
