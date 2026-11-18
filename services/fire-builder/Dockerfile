FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/fire-builder ./services/fire-builder
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/fire-builder /fire-builder
COPY services/fire-builder/data/provinces.json /data/provinces.json
ENV PROVINCE_REFERENCE_PATH=/data/provinces.json
EXPOSE 8130
ENTRYPOINT ["/fire-builder"]
