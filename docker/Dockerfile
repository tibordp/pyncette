FROM python:3.8

RUN apt-get update && apt-get install -y \
    dumb-init \
 && rm -rf /var/lib/apt/lists/*

ADD . /opt/pyncette
RUN pip install /opt/pyncette[redis,postgres,prometheus] && rm -rf /opt/

WORKDIR /pyncette
ADD docker/app.py app.py

EXPOSE 9699/tcp

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["python", "app.py"]