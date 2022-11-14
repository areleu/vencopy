import pytest
import pandas as pd
from vencopy.workflows import standard

@pytest.fixture(scope='module')
def ref():
    return pd.read_hdf('./test/validate/postFlex.h5f', key='postFlex')

@pytest.fixture(scope='module')
def data():
    return standard(returnValidation=True)

@pytest.mark.validate
def test_isFirstActivity(ref, data):
    assert ((ref['isFirstActivity'].astype(int) - data['isFirstActivity'].astype(int)).abs() <= 1e-6).all()

@pytest.mark.validate
def test_isLastActivity(ref, data):
    assert ((ref['isLastActivity'].astype(int) - data['isLastActivity'].astype(int)).abs() <= 1e-6).all()
    