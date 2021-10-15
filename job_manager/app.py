import logging
from sqlalchemy.exc import IntegrityError
from fastapi import FastAPI,BackgroundTasks,Response,Depends

from . import db, models, caching, crud, settings, parsing, remotes

try:
    logging.basicConfig(level=getattr(logging,settings.LOG_LEVEL))
except AttributeError:
    pass

logger = logging.getLogger(__name__)

cache = caching.RESTCache(
            settings.DATA_CACHE_URL + "/files"
        )

api = remotes.Api(settings.ROUTER_URL)

try:
    models.Base.metadata.create_all(db.engine)
except IntegrityError:
    pass

app = FastAPI()

def get_sess():
    sess = db.Session()
    try:
        yield sess
    finally:
        sess.close()

@app.get("/job/")
def list_jobs(session = Depends(get_sess)):
    jobs = session.query(models.Job).all()
    repr = {"jobs":[job.path for job in jobs]}
    return repr

@app.get("/job/{path:path}")
def dispatch(path:str,
        background_tasks: BackgroundTasks, session = Depends(get_sess)):
    try:
        result = cache.get(path)
    except caching.NotCached:
        logger.info("%s not cached", path)
    else:
        logger.info("Returning %s from cache", path)
        return Response(result, media_type="application/octet-stream")

    try:
        job = models.Job(path)
    except parsing.ParsingError as pe:
        logger.warning("Malformed path %s: %s", path, str(pe))
        return Response(str(pe), status_code=404)

    for subjob in job.subjobs():
        try:
            assert (error := session.query(models.Error).get(subjob.path)) is None
        except AssertionError:
            logger.warning("%s returned %s", subjob.path, error.status_code)
            return Response(
                    f"Posted at {error.timestamp}: " + error.content,
                    status_code = error.status_code
                )

    background_tasks.add_task(crud.handle_job,
            settings.JOB_TIMEOUT, settings.JOB_RETRY,
            session, cache, api,
            job
        )

    return Response("Handling job",status_code=202)

@app.get("/errors/")
def list_errors(session = Depends(get_sess)):
    return {"errors":[e.json() for e in session.query(models.Error).all()]}

@app.get("/errors/purge")
def purge_errors(session = Depends(get_sess)):
    errors = session.query(models.Error).all()
    for error in errors:
        session.delete(error)
    session.commit()
    return {"deleted":len(errors)}
