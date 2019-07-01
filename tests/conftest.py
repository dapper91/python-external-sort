import random

import pytest


@pytest.fixture(scope='session')
def freezed_random(request):
    random.seed(a=request.node.name)

    return random
