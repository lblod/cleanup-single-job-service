FROM semtech/mu-javascript-template:1.9.1
LABEL maintainer="Nordine Bittich <contact@bittich.be>"
ENV SUDO_QUERY_RETRY="true"
ENV SUDO_QUERY_RETRY_FOR_HTTP_STATUS_CODES="404,500,503"
# see https://github.com/mu-semtech/mu-javascript-template for more info
