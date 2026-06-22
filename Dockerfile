ARG BASE_IMAGE=alpine:3.23.4
FROM ${BASE_IMAGE}

# 安装必要的运行时工具
RUN apk add --no-cache ca-certificates tzdata curl

# 创建应用用户
RUN addgroup -S app && adduser -S -G app app

WORKDIR /app

# 1. 直接复制编译好的二进制文件（在 Jenkins 构建时复制到当前目录的 bin/ 下）
COPY bin/server /app/bin/server

# 2. 复制配置文件和静态文件
COPY configs /app/configs

# 3. 创建日志目录并设置权限
RUN mkdir -p /app/logs \
 && chown -R app:app /app \
 && chmod -R 755 /app

USER app

EXPOSE 19901

# 健康检查（使用 curl 更可靠）
# HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
#   CMD curl -f http://127.0.0.1:8080/v1/health || exit 1

# 默认启动命令
CMD ["/app/bin/server", "-c", "configs/config.toml"]