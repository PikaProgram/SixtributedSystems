FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/bmkg ./services/bmkg-mock
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/bmkg /bmkg
EXPOSE 8081
ENTRYPOINT ["/bmkg"]
