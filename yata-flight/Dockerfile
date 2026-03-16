# Lightweight runtime — expects cross-compiled binary.
# Build: cargo zigbuild --release -p yata-flight --target x86_64-unknown-linux-gnu.2.17
# Then:  docker buildx build --platform linux/amd64 -f yata/yata-flight/Dockerfile -t ghcr.io/gftdcojp/yata-flight:<tag> --push .
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY yata/target/x86_64-unknown-linux-gnu/release/yata-flight /usr/local/bin/yata-flight
RUN mkdir -p /data/lance /data/graph
EXPOSE 32010
ENTRYPOINT ["/usr/local/bin/yata-flight"]
