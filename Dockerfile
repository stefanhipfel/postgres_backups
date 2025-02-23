FROM golang:1.12.0-alpine3.9 as builder
WORKDIR /go/src/github.com/stefanhipfel/postgres-backup
RUN apk add --no-cache make
COPY . .
ARG VERSION
RUN make all

FROM alpine:3.9
LABEL maintainer="Stefan Hipfel <stefan.hipfel@gmail.com>"

RUN apk add --no-cache curl
RUN curl -Lo /bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
	&& chmod +x /bin/dumb-init \
	&& dumb-init -V
COPY --from=builder /go/src/github.com/stefanhipfel/postgres-backup/bin/linux/postgres-backup /usr/local/bin/
ENTRYPOINT ["dumb-init", "--"]
CMD ["postgres-backup"]