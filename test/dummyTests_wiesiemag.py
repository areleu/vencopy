import pytest

# DECORATORS
# validate = pytest.mark.validate              # Simple MarkDecorator
#mark2 = pytest.mark.NAME(name1=value) # Parametrized MarkDecorator

@pytest.fixture
def a():
    return 3


@pytest.mark.parametrize('a', [1, 2, 3, 4, 5])
def test_uncontrolled_charging(a):
    #    a = 3
    assert a > 4


@pytest.mark.validate
@pytest.mark.xfail(strict=True)
def test_expected_fail():
    # raise TypeError()
    pass
