#!/bin/bash

cd /home/oliveirmic/Projetos-dev/BIREME-GIT/ScalaMongoDiff/ || exit

SBT_HOME=/home/users/operacao/.local/share/coursier/bin

export SBT_OPTS="-Xms12g -Xmx18g -XX:+UseG1GC"

$SBT_HOME/sbt "runMain MongoDBCollDiff $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14} ${15} ${16} ${17} ${18} ${19} ${20} ${21} ${22} ${23} ${24} ${25}"
ret="$?"

cd - || exit

exit $ret