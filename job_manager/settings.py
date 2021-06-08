"""
Required settings:
* Env:
    - KEY_VAULT_URL
* Config:
    - ROUTER_URL
    - JOB_TIMEOUT
    - JOB_RETRY
"""
import environs
from fitin import views_config

env = environs.Env()
env.read_env()
config = fitin.seek_config([
        fitin.dict_resolver({"JOB_TIMEOUT":4000}),
        views_config(env.str("KEY_VAULT_URL"))
    ])
