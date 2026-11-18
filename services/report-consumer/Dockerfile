FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/report-consumer ./services/report-consumer
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/report-consumer /report-consumer
ENTRYPOINT ["/report-consumer"]
