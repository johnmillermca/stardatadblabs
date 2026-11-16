FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY main.go .
RUN go mod init stardata-claims && go build -o /stardata-claims .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /stardata-claims /stardata-claims
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/stardata-claims"]
