FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/auth ./services/auth-service
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/auth /auth
EXPOSE 8091
ENTRYPOINT ["/auth"]
