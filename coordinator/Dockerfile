# syntax=docker/dockerfile:1
# Two stages: a build image with the toolchain and vcpkg, and a runtime image
# holding only the binary.

FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates cmake curl git jq ninja-build pkg-config \
        tar unzip zip \
    && rm -rf /var/lib/apt/lists/*

# vcpkg needs the system cmake/ninja on arm64 Linux (Docker on Apple silicon).
ENV VCPKG_ROOT=/opt/vcpkg VCPKG_FORCE_SYSTEM_BINARIES=1
WORKDIR /src

# Dependencies first, in their own layer: it is rebuilt only when vcpkg.json
# changes. vcpkg is checked out at the manifest's baseline, as in CI.
COPY vcpkg.json ./
RUN --mount=type=cache,target=/root/.cache/vcpkg \
    git clone --filter=tree:0 https://github.com/microsoft/vcpkg "$VCPKG_ROOT" \
    && git -C "$VCPKG_ROOT" checkout "$(jq -r '."builtin-baseline"' vcpkg.json)" \
    && "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics \
    && "$VCPKG_ROOT/vcpkg" install --x-install-root=build/release/vcpkg_installed

COPY . .
RUN cmake --preset release -DBUILD_TESTING=OFF \
    && cmake --build --preset release --target coordinator

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/release/coordinator /usr/local/bin/coordinator
# Where compose.yaml mounts the database volume; owned by the runtime user.
RUN mkdir -p /var/lib/orch && chown ubuntu:ubuntu /var/lib/orch
USER ubuntu
EXPOSE 8080
HEALTHCHECK --interval=2s --timeout=2s --retries=15 \
    CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/coordinator"]
