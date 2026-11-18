FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/store ./services/store
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/store /store
EXPOSE 8100
ENTRYPOINT ["/store"]
