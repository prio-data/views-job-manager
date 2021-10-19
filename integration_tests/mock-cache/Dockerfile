
FROM prioreg.azurecr.io/prio-data/uvicorn_deployment:1.3.0
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY app.py app.py
ENV APP="app:app"
