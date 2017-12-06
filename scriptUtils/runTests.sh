export DISABLE_PROVENANCE=TRUE
export MAVEN_OPTS="-Xss1500m"
../build/mvn  -Phadoop-2.7 -Dhadoop.version=2.7.3  compile package