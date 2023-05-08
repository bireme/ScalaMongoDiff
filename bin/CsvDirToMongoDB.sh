#!/bin/bash

cd /home/javaapps/sbt-projects/ScalaMongoDiff/ || exit

export SBT_OPTS="-Xms12g -Xmx18g -XX:+UseG1GC"

sbt "runMain MongoDBCollDiff_Test"
ret="$?"

cd - || exit

exit $ret