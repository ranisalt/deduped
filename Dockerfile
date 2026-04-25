# syntax=docker/dockerfile:1

# Stage 1: Builder
FROM docker.io/alpine:3.23 AS builder

ARG VCPKG_REF=2026.03.18
ARG ENABLE_LTO=ON
ARG ENABLE_UNITY_BUILD=ON

RUN apk add --no-cache \
    build-base \
    cmake \
    ninja \
    git \
    curl \
    zip \
    unzip \
    linux-headers \
    pkgconfig

ENV CXXFLAGS="-fPIC" CFLAGS="-fPIC"

RUN git clone --filter=tree:0 https://github.com/microsoft/vcpkg.git /opt/vcpkg && \
    cd /opt/vcpkg && \
    git checkout "${VCPKG_REF}" && \
    ./bootstrap-vcpkg.sh -disableMetrics

ENV VCPKG_ROOT=/opt/vcpkg
ENV VCPKG_FORCE_SYSTEM_BINARIES=1

WORKDIR /build
COPY . .

RUN mkdir -p /build/cmake-build && \
    cmake --preset default -B /build/cmake-build -DENABLE_LTO=${ENABLE_LTO} -DENABLE_UNITY_BUILD=${ENABLE_UNITY_BUILD} && \
    cmake --build /build/cmake-build --config Release --parallel $(nproc)

# Stage 2: Runtime
FROM ghcr.io/linuxserver/baseimage-alpine:3.23

RUN apk add --no-cache libstdc++

COPY --from=builder --chmod=755 /build/cmake-build/Release/deduped /usr/local/bin/
COPY --from=builder --chmod=755 /build/cmake-build/Release/deduped-cli /usr/local/bin/

COPY root/ /

LABEL maintainer=ranisalt version=0.1.0
