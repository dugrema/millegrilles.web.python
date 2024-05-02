FROM docker.maple.maceroc.com:5000/millegrilles_messages_python:2024.4.24

ENV CERT_PEM=/run/secrets/cert.pem \
    KEY_PEM=/run/secrets/key.pem \
    CA_PEM=/run/secrets/pki.millegrille.cert \
    MQ_HOSTNAME=mq \
    MQ_PORT=5673 \
    REDIS_HOSTNAME=redis \
    REDIS_PASSWORD_PATH=/var/run/secrets/passwd.redis.txt \
    WEB_PORT=1443

EXPOSE 80 443

# Creer repertoire app, copier fichiers
COPY . $BUILD_FOLDER

# Pour offline build
#ENV PIP_FIND_LINKS=$BUILD_FOLDER/pip \
#    PIP_RETRIES=0 \
#    PIP_NO_INDEX=true

RUN pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt && \
    cd $BUILD_FOLDER/  && \
    python3 ./setup.py install

CMD ["-m", "server", "--verbose"]
