# 构建阶段
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/ai-api-proxy ./cmd/ai-api-proxy

# 运行阶段:单目录挂载 /data(宿主 -v <dir>:/data)
#   /data/config/config.yaml   配置(缺失时启动自动释放内嵌示例)
#   /data/lib/                 数据(SQLite/会话;config.data_dir 指向此处)
#   /data/plugins/             插件包热载目录(config.plugins_dir 指向此处)
FROM alpine:3.20
RUN apk add --no-cache tzdata \
    && mkdir -p /data/config /data/lib /data/plugins
COPY --from=build /out/ai-api-proxy /usr/local/bin/ai-api-proxy
WORKDIR /data
EXPOSE 8080
ENTRYPOINT ["ai-api-proxy"]
CMD ["-config", "/data/config/config.yaml"]
