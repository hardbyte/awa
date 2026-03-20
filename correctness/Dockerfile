FROM eclipse-temurin:21-jre

ARG TLA_VERSION=v1.8.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL \
        -o /opt/tla2tools.jar \
        "https://github.com/tlaplus/tlaplus/releases/download/${TLA_VERSION}/tla2tools.jar"

WORKDIR /work

ENTRYPOINT ["java", "-XX:+UseParallelGC", "-cp", "/opt/tla2tools.jar", "tlc2.TLC"]
