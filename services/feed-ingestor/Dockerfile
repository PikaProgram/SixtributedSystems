FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/feed-ingestor ./services/feed-ingestor
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/feed-ingestor /feed-ingestor
EXPOSE 8120
ENTRYPOINT ["/feed-ingestor"]
