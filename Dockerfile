FROM prioreg.azurecr.io/prio-data/uvicorn_deployment:2.0.0

COPY ./requirements.txt /
RUN pip install -r requirements.txt 

COPY ./job_manager/* /job_manager/
ENV GUNICORN_APP="job_manager.app:app"
