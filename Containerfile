FROM quay.io/redhat-cop/python-kopf-s2i:v1.37

USER root

COPY . /tmp/src

RUN rm -rf /tmp/src/.git* && \
    chown -R 1001 /tmp/src && \
    chgrp -R 0 /tmp/src && \
    chmod -R g+w /tmp/src && \
    install -d /tmp/scripts

USER 1001

RUN /tmp/src/.s2i/bin/assemble

CMD ["/usr/libexec/s2i/run"]
