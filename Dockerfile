FROM rust:1.83-bookworm

RUN apt-get update && apt-get install -y \
    liburing-dev \
    build-essential \
    pkg-config \
    libclang-dev \
    clang \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN cargo build --release --features io_uring --example benchmark_iouring

CMD ["./target/release/examples/benchmark_iouring"]
