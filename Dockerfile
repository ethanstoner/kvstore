# syntax=docker/dockerfile:1

# ---- Stage 1: build ----
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /build

# Cache dependencies first (separate layer)
COPY pom.xml .
RUN mvn dependency:go-offline -B || true

# Build
COPY src ./src
RUN mvn package -DskipTests -B

# ---- Stage 2: runtime ----
FROM eclipse-temurin:21-jre-jammy

LABEL org.opencontainers.image.title="kvstore"
LABEL org.opencontainers.image.description="LSM-tree key-value store with Redis-compatible RESP server"
LABEL org.opencontainers.image.source="https://github.com/ethanstoner/kvstore"

# Create non-root user
RUN groupadd -r kvstore && useradd -r -g kvstore kvstore

WORKDIR /app
COPY --from=builder /build/target/kvstore-0.1.0.jar /app/kvstore.jar

# Data directory mounted as a volume
RUN mkdir -p /data && chown kvstore:kvstore /data
VOLUME ["/data"]

USER kvstore

EXPOSE 6379

# Default: serve on 0.0.0.0:6379 with persistent data at /data
ENTRYPOINT ["java", "-jar", "/app/kvstore.jar"]
CMD ["serve", "--port", "6379", "--data", "/data"]
