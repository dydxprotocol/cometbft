# We need to build in a Linux environment to support C libraries, e.g. RocksDB.
# We use Debian instead of Alpine, so that we can use binary database packages
# instead of spending time compiling them.
FROM cometbft/cometbft-db-testing:v0.14.3

RUN apt-get -qq update -y && apt-get -qq upgrade -y >/dev/null

# Clean install of Go 1.23.5 (wipe any previous Go first!)
RUN rm -rf /usr/local/go \
 && curl -fsSL https://go.dev/dl/go1.23.5.linux-amd64.tar.gz \
 | tar -C /usr/local -xz
ENV PATH=/usr/local/go/bin:$PATH

# Pin toolchain to avoid auto-downloads (either of these is fine)
ENV GOTOOLCHAIN=local
# or: ENV GOTOOLCHAIN=go1.23.5

# Set up build directory /src/cometbft
ENV COMETBFT_BUILD_OPTIONS badgerdb,boltdb,cleveldb
WORKDIR /src/cometbft

# Fetch dependencies separately (for layer caching)
COPY go.mod go.sum ./
RUN go mod download

# Build CometBFT and install into /usr/bin/cometbft
COPY . .
RUN go version && which go && go env GOTOOLCHAIN && go env GOFLAGS
RUN make build && cp build/cometbft /usr/bin/cometbft
COPY test/e2e/docker/entrypoint* /usr/bin/
RUN cd test/e2e && make node && cp build/node /usr/bin/app

# Set up runtime directory. We don't use a separate runtime image since we need
# e.g. leveldb and rocksdb which are already installed in the build image.
WORKDIR /cometbft
VOLUME /cometbft
ENV CMTHOME=/cometbft
ENV GORACE "halt_on_error=1"

EXPOSE 26656 26657 26660 6060
ENTRYPOINT ["/usr/bin/entrypoint"]
CMD ["node"]
STOPSIGNAL SIGTERM
