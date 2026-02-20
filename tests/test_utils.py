import numpy as np
import pytest

from cc_plugin_cc6.utils import (
    convert_lon_180,
    convert_lon_360,
    convert_posix_to_python,
    crosses_anti_meridian,
    crosses_zero_meridian,
)


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
    assert python_regex == r'[!"#$%&\'\(\)*+,\-./:;<=>?@[\\\]^_`{|}~]'


def test_convert_posix_to_python_space():
    posix_regex = r"[[:space:]]"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"\s"


def test_convert_posix_to_python_quantifier():
    posix_regex = r"[[:digit:]]\{1,\}"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"\d+"


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
    assert python_regex == r"[a-zA-Z0-9]+\d+\s+hello"


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
    assert python_regex == r"[a-zA-Z0-9]+\d+hello[a-zA-Z0-9]+"


def test_convert_posix_to_python_ripf_raw():
    posix_regex = (
        r"r[[:digit:]]\{1,\}i[[:digit:]]\{1,\}p[[:digit:]]\{1,\}f[[:digit:]]\{1,\}$"
    )
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"r\d+i\d+p\d+f\d+$"


def test_convert_posix_to_python_ripf():
    posix_regex = "r[[:digit:]]\\{1,\\}i[[:digit:]]\\{1,\\}p[[:digit:]]\\{1,\\}f[[:digit:]]\\{1,\\}$"
    python_regex = convert_posix_to_python(posix_regex)
    assert python_regex == r"r\d+i\d+p\d+f\d+$"


def test_convert_lon_360():
    lon = np.array(
        [-360, -170, -125, -90, -10, 0, 10, 100, 180, 270, 360, 370, 720, 1000]
    )
    expected = np.array([0, 190, 235, 270, 350, 0, 10, 100, 180, 270, 0, 10, 0, 280])
    result = convert_lon_360(lon)
    np.testing.assert_allclose(result, expected)


def test_convert_lon_180():
    lon = np.array(
        [
            -400,
            -360,
            -270,
            -180,
            -170,
            -130,
            -75,
            -10,
            35,
            105,
            170,
            180,
            190,
            270,
            280,
            520,
            720,
            1000,
        ]
    )
    expected = np.array(
        [
            -40,
            0,
            90,
            -180,
            -170,
            -130,
            -75,
            -10,
            35,
            105,
            170,
            -180,
            -170,
            -90,
            -80,
            160,
            0,
            -80,
        ]
    )
    result = convert_lon_180(lon)
    np.testing.assert_allclose(result, expected)


def test_crosses_meridian():
    """Grid around Greenwich, only crosses 0-meridian."""
    # only crosses 0-meridian
    lon = np.array([-5, -2, 0, 2, 5])
    assert crosses_zero_meridian(lon) is True
    assert crosses_anti_meridian(lon) is False
    lon = np.arange(-160, 160, 1)
    assert crosses_zero_meridian(lon) is True
    assert crosses_anti_meridian(lon) is False
    # does not cross 0- or anti-meridian
    lon = np.array([50, 55, 60, 65])
    assert crosses_zero_meridian(lon) is False
    assert crosses_anti_meridian(lon) is False
    lon = np.arange(-160, 0, 1)
    assert crosses_zero_meridian(lon) is False
    assert crosses_anti_meridian(lon) is False
    # only crosses anti-meridian
    lon = np.array([170, 175, 179, -179, -175])
    assert crosses_zero_meridian(lon) is False
    assert crosses_anti_meridian(lon) is True
    lon = np.arange(0, 300, 1)
    assert crosses_zero_meridian(lon) is False
    assert crosses_anti_meridian(lon) is True
    # Some slightly larger NAM domain in [0, 360) ...
    lon = np.concatenate([np.arange(175, 360, 1), np.arange(0, 5, 1)])
    assert crosses_zero_meridian(lon) is True
    assert crosses_anti_meridian(lon) is True
    # ... and [-180, 180)
    lon = np.concatenate([np.arange(175, 180, 1), np.arange(-180, 6, 1)])
    assert crosses_zero_meridian(lon) is True
    assert crosses_anti_meridian(lon) is True
    # ARC / ANT domain in [0, 360) ...
    lon = np.arange(0, 360, 1)
    assert crosses_zero_meridian(lon) is True
    assert crosses_anti_meridian(lon) is True
    # ... and [-180, 180)
    lon = np.arange(-180, 180, 1)
    assert crosses_zero_meridian(lon) is True
    assert crosses_anti_meridian(lon) is True
    # 2D lon array - crosses only 0-meridian
    lon2d = np.array(
        [
            [358, 359, 0, 1],
            [355, 356, 2, 3],
            [352, 353, 5, 6],
            [349, 350, 8, 9],
            [346, 347, 11, 12],
            [343, 344, 14, 15],
        ]
    )
    assert crosses_zero_meridian(lon2d) is True
    assert crosses_anti_meridian(lon2d) is False
