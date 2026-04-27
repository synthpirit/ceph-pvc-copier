# ============================================================
# Stage 1: Build
# ============================================================
FROM docker.m.daocloud.io/library/golang:1.25-bookworm AS builder

# Add Ceph Reef apt repo (Tsinghua mirror)
RUN apt-get update -qq && apt-get install -y -qq wget gnupg && \
    wget -qO /usr/share/keyrings/ceph.gpg \
      https://mirrors.tuna.tsinghua.edu.cn/ceph/keys/release.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/ceph.gpg] \
      https://mirrors.tuna.tsinghua.edu.cn/ceph/debian-reef bookworm main" \
      > /etc/apt/sources.list.d/ceph.list && \
    apt-get update -qq && \
    apt-get install -y -qq librados-dev librbd-dev && \
    rm -rf /var/lib/apt/lists/*

ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -buildvcs=false -trimpath -o /ceph-pvc-copier .

# ============================================================
# Stage 2: Runtime (minimal image with only ceph runtime libs)
# ============================================================
FROM docker.m.daocloud.io/library/debian:bookworm-slim

RUN apt-get update -qq && apt-get install -y -qq wget gnupg && \
    wget -qO /usr/share/keyrings/ceph.gpg \
      https://mirrors.tuna.tsinghua.edu.cn/ceph/keys/release.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/ceph.gpg] \
      https://mirrors.tuna.tsinghua.edu.cn/ceph/debian-reef bookworm main" \
      > /etc/apt/sources.list.d/ceph.list && \
    apt-get update -qq && \
    apt-get install -y -qq librados2 librbd1 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /ceph-pvc-copier /usr/local/bin/ceph-pvc-copier

EXPOSE 8080
ENV SERVER_PORT=8080

ENTRYPOINT ["/usr/local/bin/ceph-pvc-copier"]
