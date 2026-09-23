# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
#
# The disposable fault-tools environment of the series Parquet measurement
# harness: NGINX in front of the S3 store, dnsmasq for the DNS faults, and
# the kernel-network tools (iproute2, iptables, tcpdump, tshark) that the
# DNS, firewall and capture probes run inside one container's own network
# namespace. It also carries the shared libraries the release df_engine
# links against, so the engine binary can be mounted read-only and run in
# the same namespace.
#
# Provision it from rust/otap-dataflow, outside any measurement lease:
#
#   docker pull ubuntu:24.04
#   docker build -t series-measure-fault-tools:local \
#       --build-arg BASE=ubuntu@sha256:<resolved digest> \
#       -f crates/validation/tests/series_parquet/fault-tools.Dockerfile \
#       crates/validation/tests/series_parquet
#
# The recipe is reproducible up to distribution package updates, so the
# evidence records the resolved base digest, every installed package
# version and the final image id rather than the mutable tag.
ARG BASE=ubuntu:24.04
FROM ${BASE}
# The base the image was built from, digest included when provisioning
# passed one, so the evidence can name it from the image alone.
ARG BASE
LABEL org.opencontainers.image.base.name="${BASE}"
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx dnsmasq dnsutils iproute2 iptables tcpdump tshark ca-certificates \
    libssl3t64 libstdc++6 procps curl && rm -rf /var/lib/apt/lists/*
CMD ["/usr/sbin/nginx", "-g", "daemon off;"]
