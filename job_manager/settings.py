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

env = environs.Env()
env.read_env()
config = env
