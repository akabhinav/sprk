# Multi-node MiniSpark image: builds the project, then runs any cluster role.
# Build stage — compile + install all modules and capture the dependency classpath.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN mvn -q -DskipTests clean install \
 && mvn -q -pl minispark-python -am dependency:build-classpath \
        -Dmdep.outputFile=/src/deps-cp.txt

# Runtime stage — JDK + Python (for the PySpark data plane) + the built tree.
FROM eclipse-temurin:21-jdk
WORKDIR /src
# python3 + cloudpickle so RDD-lambda jobs can fork a worker on this node —
# exactly real Spark's "Python on every executor" requirement.
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip \
 && pip3 install --no-cache-dir --break-system-packages cloudpickle \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src /src
COPY --from=build /root/.m2 /root/.m2
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
