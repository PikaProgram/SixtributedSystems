FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/client-api ./services/client-api
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/client-api /client-api
EXPOSE 8080
ENTRYPOINT ["/client-api"]
