# Build
FROM golang:1.24-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /commander .

# Run
FROM alpine:3.21
RUN apk add --no-cache ca-certificates curl
WORKDIR /app
COPY --from=build /commander /app/commander
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=10 CMD curl -fsS http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["/app/commander"]
