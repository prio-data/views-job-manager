# ViEWS Job Manager

The service responsible for launching and managing jobs.

## Env settings

|Key                                                          |Description                    |Default                      |
|-------------------------------------------------------------|-------------------------------|-----------------------------|
|DATA_CACHE_URL                                               |                               |                             |
|ROUTER_URL                                                   |                               |                             |
|REDIS_HOST                                                   |                               |jobman-redis                 |
|REDIS_PORT                                                   |                               |6379                         |
|REDIS_DB                                                     |                               |0                            |
|REDIS_ERROR_KEY_PREFIX                                       |                               |jobman/errors:               |
|REDIS_JOB_KEY_PREFIX                                         |                               |jobman/jobs:                 |
|LOG_LEVEL                                                    |                               |WARNING                      |
|DATABASE_CONNECTION_STRING                                   |Might be deprecated            |sqlite:///db.sqlite          |
|JOB_TIMEOUT                                                  |                               |40000                        |
|JOB_RETRY                                                    |                               |10                           |
|MAX_RETRIES                                                  |                               |50                           |

## Contributing

For information about how to contribute, see [contributing](https://www.github.com/prio-data/contributing).
