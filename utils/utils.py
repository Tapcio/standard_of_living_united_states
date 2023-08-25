"""
Generic Utils
"""
from typing import Optional
import yaml
import requests
import time


def get_config(loc: Optional[str] = None) -> dict:
    """
    Loads in-project config file, optional filelocation can be passed as well.
    :param loc: [OPTIONAL] file location
    :return: dict of yaml data
    """
    with open(loc if loc else "config.yml") as f:
        return yaml.safe_load(f)
