# config-center 开发拓扑镜像：多阶段构建产出单二进制
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/config-center ./cmd/config-center

FROM alpine:3.20
COPY --from=build /out/config-center /usr/local/bin/config-center
EXPOSE 8080
ENTRYPOINT ["config-center"]
