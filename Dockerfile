# Shopping Sessions: native C++ pipeline (src/main.cpp). Static binary, no runtime deps.
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ libc6-dev && rm -rf /var/lib/apt/lists/*
ARG TARGETARCH
COPY src/ /src/
# Scorers are Intel Xeon Cascade Lake or newer (AVX-512); arm64 only for local loops.
RUN if [ "$TARGETARCH" = "arm64" ]; then M="-mcpu=native"; else M="-march=x86-64-v3 -mtune=cascadelake"; fi && \
    g++ -std=c++17 -O3 $M -static -pthread -o /ss /src/main.cpp

FROM debian:bookworm-slim
COPY --from=build /ss /app/ss
CMD ["/app/ss"]
