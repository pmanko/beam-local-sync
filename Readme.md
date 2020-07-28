https://beam.apache.org/get-started/quickstart-java/
https://beam.apache.org/get-started/wordcount-example/
```powershell
mvn archetype:generate `
 -D archetypeGroupId=org.apache.beam `
 -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
 -D archetypeVersion=2.22.0 `
 -D groupId=org.example `
 -D artifactId=word-count-beam `
 -D version="0.1" `
 -D package=org.apache.beam.examples `
 -D interactiveMode=false
```

```powershell
mvn package exec:java -D exec.mainClass=com.dataradiant.beam.examples.WordCount `
 -D exec.args="--runner=FlinkRunner --flinkMaster=localhost:8081 --filesToStage=.\target\beam-starter-bundled-2.22.0.jar 
               --inputFile=pom.xml --output=C:\tmp\counts" -P flink-runner`
```




org.apache.beam.examples.WordCount


## Overview

### Batch Load
1. In OpenMRS: loop over all relevant patients & their data 
2. Create a bundle for each patient and send to Queue
3. Read from Queue and load into HAPI server
 

1b. Update:
