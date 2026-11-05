# ============================================================
# Fakecloud Wrapper Image
# Extends the official fakecloud image and adds curl so that
# Docker HEALTHCHECK can probe the /_fakecloud/health endpoint.
# The base fakecloud image is a minimal Rust binary with no
# HTTP tools — this thin layer adds only what's needed.
# ============================================================
FROM ghcr.io/faiscadev/fakecloud:latest

# Install curl — try Alpine (apk) first, fall back to Debian/Ubuntu (apt-get).
# One of these will succeed depending on the base image used by fakecloud.
RUN apk add --no-cache curl 2>/dev/null \
    || (apt-get update -qq && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*) \
    || echo "WARNING: could not install curl via apk or apt-get"
