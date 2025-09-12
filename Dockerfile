# --- Build with Go 1.24 ---
FROM golang:1.24 AS build
WORKDIR /src

# Cache deps
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build the main package in cmd/server  ⬅️ this is the important change
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o app ./cmd/server

# --- Minimal runtime image ---
FROM alpine:3.20
RUN adduser -D -g '' appuser
WORKDIR /app

COPY --from=build /src/app /app/app
COPY --from=build /src/index.html /app/index.html
COPY --from=build /src/static /app/static

USER appuser
ENV PORT=8080
EXPOSE 8080
CMD ["/app/app"]
