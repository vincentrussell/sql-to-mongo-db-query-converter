
release process:

mvn versions:set -DnewVersion=1.0
git add .
git commit -m "preparing for release 1.0"
git tag sql-to-mongo-db-query-converter-1.0
mvn clean deploy -P release
mvn versions:set -DnewVersion=1.1-SNAPSHOT
git add .
git commit -m "preparing for development version 1.1"
