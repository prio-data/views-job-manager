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
import fitin

env = environs.Env()
env.read_env()
config = fitin.views_config(env.str("KEY_VAULT_URL"))
