import logging
from fastapi import FastAPI,BackgroundTasks,Response,Depends

from . import db, models, caching, crud, settings, parsing, remotes

logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
logger.setLevel(logging.WARNING)

try:
    logging.basicConfig(level=getattr(logging,settings.config("LOG_LEVEL")))
except AttributeError:
    pass
logger = logging.getLogger(__name__)

cache = caching.BlobStorageCache(
        settings.config("BLOB_STORAGE_CONNECTION_STRING"),
        settings.config("BLOB_STORAGE_ROUTER_CACHE")
        )

api = remotes.Api(settings.config("ROUTER_URL"))

app = FastAPI()

def get_sess():
    sess = db.Session()
    try:
        yield sess
    finally:
        sess.close()

@app.get("/{path:path}")
def dispatch(path:str,background_tasks:BackgroundTasks,session = Depends(get_sess)):
    try:
        job = models.Job.parse_whole_path(path)
    except parsing.ParsingError as pe:
        return Response(str(pe), status_code=404)
    try:
        result = cache.get(job.path())

    except caching.NotCached:
        logger.debug("Checking for previous errors for %s",str(job))
        for job in job.subjobs():
            error = session.query(models.Error).get(job.path)
            if error is not None:
                return Response(
                        f"Posted at {error.posted_at}: {error.content}",
                        status_code = error.status_code)

        logger.info("Handling %s",str(job))
        background_tasks.add_task(crud.handle_job,
                settings.config("JOB_TIMEOUT"),
                settings.config("JOB_RETRY"),
                session,
                cache,
                api,
                job
            )
        return Response("Handling job",status_code=202)

    else:
        logger.info("Returning %s from cache",str(job))
        return Response(result,media_type="application/octet-stream")
