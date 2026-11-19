# Multi-stage, non-root image for the standard patio process.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/patio ./cmd/patio

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/patio /patio
USER nonroot:nonroot
ENTRYPOINT ["/patio"]
