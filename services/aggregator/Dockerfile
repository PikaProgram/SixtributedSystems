FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/aggregator ./services/aggregator
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/aggregator /aggregator
COPY services/aggregator/data/volcanoes.json /data/volcanoes.json
ENV VOLCANO_REFERENCE_PATH=/data/volcanoes.json
EXPOSE 8090
ENTRYPOINT ["/aggregator"]
