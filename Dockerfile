# 构建阶段
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/ai-api-proxy ./cmd/ai-api-proxy

# 运行阶段
FROM alpine:3.20
RUN adduser -D -u 10001 apx
COPY --from=build /out/ai-api-proxy /usr/local/bin/ai-api-proxy
COPY config.example.yaml /etc/ai-api-proxy/config.example.yaml
USER apx
WORKDIR /var/lib/ai-api-proxy
EXPOSE 8080
ENTRYPOINT ["ai-api-proxy"]
CMD ["-config", "/etc/ai-api-proxy/config.yaml"]
