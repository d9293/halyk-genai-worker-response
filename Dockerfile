# worker.Response image. Configuration is not baked in: mount an env file and
# pass it with --config_file; secrets come as environment variables, which win
# over the file.

FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app .

# Static binary on distroless: CA certificates, no shell, runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
EXPOSE 8084
ENTRYPOINT ["/app"]
CMD ["--config_file", "/config/app.env"]
