FROM golang:alpine as build
WORKDIR /usr/src/app

# Download Mods
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Build
COPY *.go .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -v -o /usr/local/bin/swoof ./...

# Final
FROM scratch
COPY --from=build /usr/local/bin/swoof /usr/local/bin/swoof

ENTRYPOINT [ "swoof" ]