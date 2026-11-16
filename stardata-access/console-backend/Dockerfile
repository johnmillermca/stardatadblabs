FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /console-backend .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /console-backend /console-backend
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/console-backend"]
