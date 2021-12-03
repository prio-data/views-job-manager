# ViEWS Job Manager

The service responsible for launching and managing jobs.

## Env settings

|Key                     |Description                                                  |Default                      |
|------------------------|-------------------------------------------------------------|-----------------------------|
|DATA_CACHE_URL          |URL to an instance of restblobs                              |http://data-cache            |
|ROUTER_URL              |URL to an instance of views_router                           |http://router                |
|REDIS_HOST              |Hostname of redis instance                                   |jobman-redis                 |
|REDIS_PORT              |Port of redis instance                                       |6379                         |
|REDIS_DB                |DBNO of redis instance                                       |0                            |
|REDIS_ERROR_KEY_PREFIX  |Prefix to add to error keys                                  |jobman/errors:               |
|REDIS_JOB_KEY_PREFIX    |Prefix to add to job keys                                    |jobman/jobs:                 |
|LOG_LEVEL               |Python log level                                             |WARNING                      |
|MAX_RETRIES             |Max prerequisite job await tries before failing              |50                           |
|RETRY_SLEEP             |Time to wait between each job retry                          |5                            |
|CHECK_ERRORS_EVERY      |Check errors for prerequisite every N retries                |5                            |

## Contributing

For information about how to contribute, see [contributing](https://www.github.com/prio-data/contributing).
