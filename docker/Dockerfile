FROM python:3.8

RUN apt-get update && apt-get install -y \
    dumb-init \
 && rm -rf /var/lib/apt/lists/*

ADD . /opt/pyncette
RUN pip install /opt/pyncette[all] && rm -rf /opt/

WORKDIR /pyncette
ADD ./docker/entrypoint.py entrypoint.py

EXPOSE 9699/tcp
ENV USE_UVLOOP=1

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["python", "entrypoint.py"]