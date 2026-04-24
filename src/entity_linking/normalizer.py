import re

punctuations_to_remove = [
    (re.compile(r"\bL\.L\.C\.?\b", re.IGNORECASE), "LLC"),
    (re.compile(r"\bL\.P\.?\b",    re.IGNORECASE), "LP"),
    (re.compile(r"\bP\.C\.?\b",    re.IGNORECASE), "PC"),
    (re.compile(r"\bInc\.",        re.IGNORECASE), "Inc"),
    (re.compile(r"\bCo\.",         re.IGNORECASE), "Co"),
    (re.compile(r"\bLtd\.",        re.IGNORECASE), "Ltd"),
]

suffixes = [
    "PROFESSIONAL CORPORATION", "LIMITED LIABILITY COMPANY", "LIMITED LIABILITY CO",
    "INCORPORATED", "CORPORATION", "A COOPERATIVE", "COOPERATIVE",
    "INCORPORATD", "INCORPORAT",
    "SERVICES",
    "CORP", "INC", "LLC", "LC", "LLP", "LP", "LTD", "LIMITED",
    "COMPANY", "CO", "PC",
    "REAL ESTATE INVESTMENT TRUST", "TRUST",
]

suffix_pattern_to_remove = re.compile(
    r"[,.]?\s+(" + "|".join(re.escape(s) for s in sorted(suffixes, key=len, reverse=True)) + r")\.?\s*$"
)

name_abbreviations = {
    r"\bST\b":       "SAINT",
    r"\bDEPT\b":     "DEPARTMENT",
    r"\bUNIV\b":     "UNIVERSITY",
    r"\bFINCL\b":    "FINANCIAL",
    r"\bGRP\b":      "GROUP",
    r"\bMFG\b":      "MANUFACTURING",
    r"\bASSOC\b":    "ASSOCIATES",
    r"\bINDUS\b":    "INDUSTRIES",
    r"\bSYS\b":      "SYSTEMS",
    r"\bTECH\b":     "TECHNOLOGIES",
    r"\bAUTO\b":     "AUTOMOTIVE",
    r"\bFED\b":      "FEDERAL",
    r"\bMGMT\b":     "MANAGEMENT",
}

name_abbreviation_to_replace = [(re.compile(pat), repl) for pat, repl in name_abbreviations.items()]


def normalize_name(name: str | None) -> str:
    if not name or not isinstance(name, str):
        return ""
    
    text = name
    for pattern, replacement in punctuations_to_remove:
        text = pattern.sub(replacement, text)

    text = text.upper()

    text = text.replace("&", "AND")
    text = re.sub(r"([A-Z])-([A-Z])", r"\1 \2", text)

    text = re.sub(r"[^A-Z0-9 ]", " ", text)

    text = re.sub(r"\s+", " ", text).strip()

    for pattern, replacement in name_abbreviation_to_replace:
        text = pattern.sub(replacement, text)
    text = re.sub(r"\s+", " ", text).strip()

    max_iterations = 5
    iterations = 0
    
    while iterations < max_iterations:
        new_text = suffix_pattern_to_remove.sub("", text).strip()
        if new_text == text:
            break
        text = new_text
        iterations += 1

    return text

street_abbreviations = {
    r"\bSTREET\b": "ST",
    r"\bROAD\b": "RD",
    r"\bAVENUE\b": "AVE",
    r"\bBOULEVARD\b": "BLVD",
    r"\bDRIVE\b": "DR",
    r"\bLANE\b": "LN",
    r"\bCOURT\b": "CT",
    r"\bPLACE\b": "PL",
    r"\bSUITE\b": "STE",
    r"\bAPARTMENT\b": "APT",
    r"\bBUILDING\b": "BLDG",
    r"\bFLOOR\b": "FL",
    r"\bNORTH\b": "N",
    r"\bSOUTH\b": "S",
    r"\bEAST\b": "E",
    r"\bWEST\b": "W",
}


street_abbreviation_to_replace = [(re.compile(k, re.IGNORECASE), v) for k, v in street_abbreviations.items()]


def normalize_address(street: str | None) -> str:
    if not street or not isinstance(street, str):
        return ""
    text = street.upper()
    for pattern, replacement in street_abbreviation_to_replace:
        text = pattern.sub(replacement, text)
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text



def normalize_zip(zip_code: str | None) -> str:
    return zip_code.strip()[:5]