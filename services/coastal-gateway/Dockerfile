FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/coastal-gateway ./services/coastal-gateway
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/coastal-gateway /coastal-gateway
EXPOSE 8110
ENTRYPOINT ["/coastal-gateway"]
