FROM openjdk:17
WORKDIR /tmp
COPY /out/artifacts/Operations_CRUD_jar /tmp
COPY entrypoint.sh /tmp/entrypoint.sh

ENTRYPOINT ["/tmp/entrypoint.sh"]
CMD ["sh"]
