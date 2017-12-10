#!/usr/bin/env bash
export ENABLE_PROVENANCE=FALSE
export MAVEN_OPTS="-Xss1500m"
../build/mvn  -Phadoop-2.7 -Dhadoop.version=2.7.3  compile package