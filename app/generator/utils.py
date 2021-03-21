import json
import os
import time
from functools import partial, wraps
from typing import Callable, Dict

LANG: str = os.environ['LANG']
DICTIONARY: Dict[str, Dict[str, str]] = json.load(open('dictionary.json', 'r', encoding='utf-8'))


def paused(f: Callable = None, seconds: float = 1):
    if not f:
        return partial(paused, seconds=seconds)

    @wraps(f)
    def wrapper(*args, **kwargs):
        await_time = time.time()

        if wrapper.previous_timestamp:
            await_time = wrapper.previous_timestamp + seconds

        while await_time > time.time():
            time.sleep(0.1)

        result = f(*args, **kwargs)
        wrapper.previous_timestamp = time.time()

        return result

    wrapper.previous_timestamp = None

    return wrapper


def translate(word: str) -> str:
    return DICTIONARY.get(word, {}).get(LANG) or word
