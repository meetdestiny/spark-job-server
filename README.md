Spark Job Server migrated to use Spark 1.0.0-SNAPSHOT

Works well with hue Spark Igniter.

<h1>Build</h1>

1. Clone Spark repository from https://github.com/apache/spark 
2. Create jobserver folder
3. Clone this repository inside jobserver folder
4. Build using Maven 


mvn -Pyarn -Dhadoop.version=2.2.0 -Dyarn.version=2.2.0 install<br/>
mvn -Pyarn -Dhadoop.version=2.2.0 -Dyarn.version=2.2.0 package


The package goal produces a fat jar with all the dependecies bundled in one single jar. 

<h3>Starting the jobserver</h3>

java -cp jobserver-1.0.0-incubating-SNAPSHOT.jar spark.jobserver.JobServer


<h1>Credits </h1>

This is a derivative work. All credits goes to <a href="https://github.com/velvia">velvia</a> who open sourced the original job server. The <a href="https://github.com/apache/incubator-spark/pull/show/222/files/0c9c5a7">PR 222 </a>is  outdated and I needed a quick solution. This is a working POC for migrating the job server to scala 2.10.3 and sbt 0.13. Also, the original job server is sbt based. This is mavenized. 

<h1>License</h1>
Do whatever makes you happy with this code. No liabilities on me whatsoever. 







