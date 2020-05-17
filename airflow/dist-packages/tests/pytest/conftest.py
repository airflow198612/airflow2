import pytest

def pytest_addoption(parser):
    parser.addoption("--auth", action="store", default="none", help="paste your auth key")


@pytest.fixture
def auth(request):
    return request.config.option.auth

#def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
#    option_value = metafunc.config.option.name
#    if 'auth' in metafunc.fixturenames and option_value is not None:
#        metafunc.parametrize("auth", [option_value])