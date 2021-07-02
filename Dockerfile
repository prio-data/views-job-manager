FROM curlimages/curl:latest as fetch-cert
USER root
RUN curl https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem --output /root.crt

FROM prioreg.azurecr.io/uvicorn-deployment 

COPY ./requirements.txt /
RUN pip install -r requirements.txt 

RUN mkdir /certs
COPY --from=fetch-cert /root.crt /.postgresql/root.crt

COPY ./job_manager/* /job_manager/
ENV APP="job_manager.app:app"
