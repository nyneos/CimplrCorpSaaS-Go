#############################################
# Stage 1: Build Stage
#############################################
FROM golang:tip-trixie AS build  

# Set working directory
WORKDIR /app

# Copy Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod tidy

# Copy the full source code
COPY . .

# Build the Go binary (static build for Linux AMD64)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main cmd/main.go


#############################################
# Stage 2: Final Runtime Image
#############################################
FROM alpine:3.20

# Install SSL certificates (required for HTTPS calls)
RUN apk --no-cache add ca-certificates

# Set working directory for final image
WORKDIR /app

# Copy the built binary and configuration
COPY --from=build /app/main /app/
COPY --from=build /app/services.yaml /app/

# Create logs directory for runtime logs (avoid build failure)
RUN mkdir -p /app/logs

# Copy internal packages if your app loads static files or templates from there
COPY --from=build /app/internal /app/internal/

# Expose application ports (as per your services.yaml)
EXPOSE 8081 9090 3143 4143 5143 6143 2143

# Start the Go application
CMD ["/app/main"]
