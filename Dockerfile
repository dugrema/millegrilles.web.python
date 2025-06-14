FROM registry.millegrilles.com/millegrilles/messages_python:2025.4.104 as stage1

ENV CERT_PATH=/run/secrets/cert.pem \
    KEY_PATH=/run/secrets/key.pem \
    CA_PATH=/run/secrets/millegrille.cert.pem \
    MQ_HOSTNAME=mq \
    MQ_PORT=5673 \
    REDIS_HOSTNAME=redis \
    REDIS_PASSWORD_PATH=/var/run/secrets/passwd.redis.txt \
    WEB_PORT=1443

# Install pip requirements
COPY requirements.txt $BUILD_FOLDER/requirements.txt
RUN pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt

FROM stage1

ARG VBUILD=2025.4.0

EXPOSE 80 443

# Creer repertoire app, copier fichiers
COPY . $BUILD_FOLDER

# Pour offline build
#ENV PIP_FIND_LINKS=$BUILD_FOLDER/pip \
#    PIP_RETRIES=0 \
#    PIP_NO_INDEX=true

RUN cd $BUILD_FOLDER/  && \
    python3 ./setup.py install

CMD ["-m", "server", "--verbose"]
