import re

import numpy as np


def convert_posix_to_python(posix_regex):
    """
    Convert common POSIX regular expressions to Python regular expressions.

    Args:
        posix_regex (str): The POSIX regular expression to convert.

    Returns:
        str: The converted Python regular expression.

    Raises:
        ValueError: If the input is not a string or contains invalid POSIX character classes.
    """
    if not isinstance(posix_regex, str):
        raise ValueError("Input must be a string")

    # [a,b] -> [a b]
    posix_regex = re.sub(
        r"(?<!\\)\[(.*?)(?<!\\)\]",
        lambda m: "[" + m.group(1).replace(",", " ") + "]",
        posix_regex,
    )

    # Dictionary of POSIX to Python character class conversions
    posix_to_python_classes = {
        r"[[:alnum:]]": r"[a-zA-Z0-9]",
        r"[[:alpha:]]": r"[a-zA-Z]",
        r"[[:digit:]]": r"\d",
        r"[[:xdigit:]]": r"[0-9a-fA-F]",
        r"[[:lower:]]": r"[a-z]",
        r"[[:upper:]]": r"[A-Z]",
        r"[[:blank:]]": r"[ \t]",
        r"[[:space:]]": r"\s",
        r"[[:punct:]]": r'[!"#$%&\'()*+,\-./:;<=>?@[\\\]^_`{|}~]',
        r"[[:word:]]": r"\w",
    }

    # Replace POSIX character classes with Python equivalents
    for posix_class, python_class in posix_to_python_classes.items():
        posix_regex = posix_regex.replace(posix_class, python_class)

    # Replace POSIX quantifiers with Python equivalents
    posix_regex = posix_regex.replace(r"\{", "{").replace(r"\}", "}")

    # Deal with groups
    # Temporarily mark existing escaped parentheses that should become groups, e.g., \(
    posix_regex = re.sub(r"\\\(", "__GROUP_LEFTPARENTHESIS__", posix_regex)
    posix_regex = re.sub(r"\\\)", "__GROUP_RIGHTPARENTHESIS__", posix_regex)
    # Escape all remaining unescaped parentheses (-> literal parentheses)
    posix_regex = re.sub(r"(?<!\\)\(", r"\(", posix_regex)
    posix_regex = re.sub(r"(?<!\\)\)", r"\)", posix_regex)
    # Restore the placeholders as unescaped parentheses (grouping)
    posix_regex = posix_regex.replace("__GROUP_LEFTPARENTHESIS__", "(")
    posix_regex = posix_regex.replace("__GROUP_RIGHTPARENTHESIS__", ")")

    # Replace "{1,}" with "+" to matchone or more repetitions
    posix_regex = re.sub(r"\{1,\}", "+", posix_regex)

    return posix_regex


def match_pattern_or_string(pattern, target):
    """
    Compare a regex pattern or a string with the target string.

    Args:
        pattern (str): The regex pattern or string to compare.
        target (str): The string to compare against.

    Returns:
        bool: True if the target matches the regex pattern or is equal to the string.
    """
    try:
        return bool(
            re.fullmatch(convert_posix_to_python(pattern), target, flags=re.ASCII)
        ) or (
            pattern == target
            and convert_posix_to_python(target) == target
            and ".*" not in target
        )
    except re.error:
        # Invalid regex treat pattern as literal string
        return (
            pattern == target
            and convert_posix_to_python(target) == target
            and ".*" not in target
        )


def to_str(val):
    """
    Decode byte strings to utf-8 if possible and leave other typed input unchanged.
    """
    if isinstance(val, (bytes, np.bytes_)):
        try:
            return val.decode("utf-8")
        except UnicodeDecodeError:
            return val
    return str(val)


def sanitize(obj):
    """
    Make sure all values are json-serializable.
    """
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    return obj
