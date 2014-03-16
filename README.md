Spark Job Server migrated to use Spark 1.0.0-SNAPSHOT

Works well with hue 

<h1>Build</h1>

mvn -Pyarn -Dhadoop.version=2.2.0 -Dyarn.version=2.2.0 install
mvn -Pyarn -Dhadoop.version=2.2.0 -Dyarn.version=2.2.0 package


