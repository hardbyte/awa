FROM haproxy:3.1-alpine

USER root
RUN apk add --no-cache postgresql17-client

COPY haproxy.cfg /usr/local/etc/haproxy/haproxy.cfg
COPY check-primary.sh /usr/local/bin/check-primary.sh

RUN chmod +x /usr/local/bin/check-primary.sh

USER haproxy
