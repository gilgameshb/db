FROM openjdk:17
WORKDIR /tmp
COPY /out/artifacts/Operations_CRUD_jar /tmp
ADD entrypoint.sh /tmp/entrypoint.sh

ENTRYPOINT ["sh", "/tmp/entrypoint.sh"]
