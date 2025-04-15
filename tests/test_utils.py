import pytest

from cc_plugin_cc6.utils import convert_posix_to_python


def test_convert_posix_to_python_digit():
    posix_regex = r"[[:digit:]]"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"\d"


def test_convert_posix_to_python_alnum():
    posix_regex = r"[[:alnum:]]"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"[a-zA-Z0-9]"


def test_convert_posix_to_python_word():
    posix_regex = r"[[:word:]]"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"\w"


def test_convert_posix_to_python_punct():
    posix_regex = r"[[:punct:]]"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r'[!"#$%&\'()*+,\-./:;<=>?@[\\\]^_`{|}~]'


def test_convert_posix_to_python_space():
    posix_regex = r"[[:space:]]"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"\s"


def test_convert_posix_to_python_quantifier():
    posix_regex = r"[[:digit:]]\{1,\}"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"\d{1,}"


def test_convert_posix_to_python_invalid_input():
    with pytest.raises(ValueError):
        convert_posix_to_python(None)


def test_convert_posix_to_python_empty_string():
    posix_regex = ""
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == ""


def test_convert_posix_to_python_no_conversion_needed():
    posix_regex = r"\d+"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"\d+"


def test_convert_posix_to_python_longer_testcase():
    posix_regex = r"[[:alnum:]]+[[:digit:]]\{1,\}[[:space:]]+hello"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"[a-zA-Z0-9]+\d{1,}\s+hello"


def test_convert_posix_to_python_no_replacement_needed():
    posix_regex = r"\d+hello[a-zA-Z0-9]"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"\d+hello[a-zA-Z0-9]"


def test_convert_posix_to_python_regular_string():
    posix_regex = "hello world"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == "hello world"


def test_convert_posix_to_python_mixed_testcase():
    posix_regex = r"[[:alnum:]]+\d+hello[a-zA-Z0-9]\{1,\}"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"[a-zA-Z0-9]+\d+hello[a-zA-Z0-9]{1,}"


def test_convert_posix_to_python_ripf_raw():
    posix_regex = (
        r"r[[:digit:]]\{1,\}i[[:digit:]]\{1,\}p[[:digit:]]\{1,\}f[[:digit:]]\{1,\}$"
    )
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"r\d{1,}i\d{1,}p\d{1,}f\d{1,}$"


def test_convert_posix_to_python_ripf():
    posix_regex = "r[[:digit:]]\\{1,\\}i[[:digit:]]\\{1,\\}p[[:digit:]]\\{1,\\}f[[:digit:]]\\{1,\\}$"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"r\d{1,}i\d{1,}p\d{1,}f\d{1,}$"
