#!/bin/sh

echo Waiting for databases...
sleep 30
echo Start operations...

java -jar /tmp/Operations_CRUD.jar | tee /tmp/crud.log