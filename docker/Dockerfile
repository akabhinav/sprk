# Multi-node MiniSpark image: builds the project, then runs any cluster role.
# Build stage — compile + install all modules and capture the dependency classpath.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN mvn -q -DskipTests clean install \
 && mvn -q -pl minispark-examples -am dependency:build-classpath \
        -Dmdep.outputFile=/src/deps-cp.txt

# Runtime stage — JDK + the built tree + resolved deps.
FROM eclipse-temurin:21-jdk
WORKDIR /src
COPY --from=build /src /src
COPY --from=build /root/.m2 /root/.m2
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
