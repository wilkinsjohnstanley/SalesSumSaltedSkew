# Hadoop MapReduce Key Salting for Data Skew

## Overview

This project demonstrates how to solve the hot key problem in Hadoop MapReduce using key salting.

A highly frequent customer ID (`CUST_HOT`) creates reducer skew because all transactions for that customer would normally be processed by a single reducer. Key salting distributes the workload across multiple reducers before recombining the results.

## Technologies

- Java
- Hadoop MapReduce
- HDFS
- Docker

## Environment

- Windows + Docker Desktop (WSL2 backend)
- Docker image: `apache/hadoop:3` (Hadoop 3.3.6, CentOS 7 base)
- JDK installed inside the container for compilation: `java-1.8.0-openjdk-devel`

## Setup your Docker Container 

1. Pull the image and start a container:
   ```
   docker pull apache/hadoop:3
   docker run -it --name hadoop-wc apache/hadoop:3 /bin/bash
   ```

2. Configure pseudo-distributed HDFS. Inside the container, write to `/opt/hadoop/etc/hadoop/`:

   `core-site.xml`:
   ```xml
   <configuration>
     <property>
       <name>fs.defaultFS</name>
       <value>hdfs://localhost:9000</value>
     </property>
   </configuration>
   ```

   `hdfs-site.xml`:
   ```xml
   <configuration>
     <property>
       <name>dfs.replication</name>
       <value>1</value>
     </property>
   </configuration>
   ```

3. Format HDFS and start daemons directly (no SSH needed for a single-node container):
   ```
   hdfs namenode -format -force
   hdfs --daemon start namenode
   hdfs --daemon start datanode
   yarn --daemon start resourcemanager
   yarn --daemon start nodemanager
   ```

4. Install a JDK for compiling (the image ships JRE-only). Requires root:
   ```
   docker exec -u root -it hadoop-wc /bin/bash
   sed -i 's|mirrorlist=|#mirrorlist=|g' /etc/yum.repos.d/CentOS-*.repo
   sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*.repo
   yum install -y java-1.8.0-openjdk-devel
   ```
   (CentOS 7 is EOL, so mirrors are redirected to `vault.centos.org`.)
5. Start Docker Desktop and run the command:
` docker start hadoop-wc`

6. Once the container is up and running, let's get into our shell and start the daemons
`hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager`
7. Verify they are running
   `ps -ef | grep -i java`

## Generate a CSV with a deliberate hot key
One customer will be generated with 5000 transactions while 20 other customers will have about 50 transactions each. 
```
cd ~
mkdir -p skew-project && cd skew-project

python - << 'EOF' > transactions.csv
import random

rows = []
# hot customer - massively overrepresented
for _ in range(5000):
    rows.append(f"CUST_HOT,{round(random.uniform(10, 500), 2)}")

# 20 normal customers
for i in range(20):
    for _ in range(50):
        rows.append(f"CUST_{i},{round(random.uniform(10, 500), 2)}")

random.shuffle(rows)
for r in rows:
    print(r)
EOF

wc -l transactions.csv
head transactions.csv
```
### Now let's write the MapReduce job 
This sums the transaction amounts per customer.
```
cat > SalesSum.java << 'EOF'
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesSum {

  public static class SalesMapper
       extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text customerId = new Text();
    private DoubleWritable amount = new DoubleWritable();

    public void map(Object key, Text value, Context context
                     ) throws IOException, InterruptedException {
      String line = value.toString();
      String[] parts = line.split(",");
      if (parts.length == 2) {
        customerId.set(parts[0]);
        amount.set(Double.parseDouble(parts[1]));
        context.write(customerId, amount);
      }
    }
  }

  public static class SalesReducer
       extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
      double sum = 0.0;
      for (DoubleWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sales sum");
    job.setJarByClass(SalesSum.class);
    job.setMapperClass(SalesMapper.class);
    job.setReducerClass(SalesReducer.class);
//Note: setNumReduceTasks(4) is deliberate — with only 1 reducer, everything goes to it anyway and skew is invisible. With 4 reducers, we should see 1 straggle badly while 3 finish fast — that's the visible symptom we want to capture.
    job.setNumReduceTasks(4);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
EOF
```
Compile and package:
```
javac -classpath $(hadoop classpath) -d . SalesSum.java
jar -cvf salessum.jar -C . .
```
Output:
```
added manifest
adding: SalesSum.java(in = 2130) (out= 709)(deflated 66%)
adding: SalesSum$SalesMapper.class(in = 1816) (out= 761)(deflated 58%)
adding: SalesSum$SalesReducer.class(in = 1755) (out= 738)(deflated 57%)
adding: transactions.csv(in = 92818) (out= 21597)(deflated 76%)
adding: SalesSum.class(in = 1479) (out= 812)(deflated 45%)
```
### Not let's put the data into HDFS and run the baseline job:
```
hdfs dfs -mkdir -p /user/hadoop/skew-input
hdfs dfs -put transactions.csv /user/hadoop/skew-input

hadoop jar salessum.jar SalesSum /user/hadoop/skew-input /user/hadoop/skew-output-baseline
```
In the output, see if you can spot "Reduce input records" under MapReduce Framework. Note that 3 of the the input counts have 200~250 records while the skewed one says "Reduce input records=5300."


Output:
```
bash-4.2$ hadoop jar salessum.jar SalesSum /user/hadoop/skew-input /user/hadoop/skew-output-baseline
2026-07-08 16:53:39 INFO  MetricsConfig:120 - Loaded properties from hadoop-metrics2.properties
2026-07-08 16:53:39 INFO  MetricsSystemImpl:378 - Scheduled Metric snapshot period at 10 second(s).
2026-07-08 16:53:39 INFO  MetricsSystemImpl:191 - JobTracker metrics system started
2026-07-08 16:53:39 WARN  JobResourceUploader:149 - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
2026-07-08 16:53:39 INFO  FileInputFormat:300 - Total input files to process : 1
2026-07-08 16:53:39 INFO  JobSubmitter:202 - number of splits:1
2026-07-08 16:53:39 INFO  JobSubmitter:298 - Submitting tokens for job: job_local1908886399_0001
2026-07-08 16:53:39 INFO  JobSubmitter:299 - Executing with tokens: []
2026-07-08 16:53:39 INFO  Job:1682 - The url to track the job: http://localhost:8080/
2026-07-08 16:53:39 INFO  Job:1727 - Running job: job_local1908886399_0001
2026-07-08 16:53:39 INFO  LocalJobRunner:501 - OutputCommitter set in config null
2026-07-08 16:53:39 INFO  PathOutputCommitterFactory:174 - No output committer factory defined, defaulting to FileOutputCommitterFactory
2026-07-08 16:53:39 INFO  FileOutputCommitter:142 - File Output Committer Algorithm version is 2
2026-07-08 16:53:39 INFO  FileOutputCommitter:157 - FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2026-07-08 16:53:39 INFO  LocalJobRunner:519 - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
2026-07-08 16:53:39 INFO  LocalJobRunner:478 - Waiting for map tasks
2026-07-08 16:53:39 INFO  LocalJobRunner:252 - Starting task: attempt_local1908886399_0001_m_000000_0
2026-07-08 16:53:39 INFO  PathOutputCommitterFactory:174 - No output committer factory defined, defaulting to FileOutputCommitterFactory
2026-07-08 16:53:39 INFO  FileOutputCommitter:142 - File Output Committer Algorithm version is 2
2026-07-08 16:53:39 INFO  FileOutputCommitter:157 - FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2026-07-08 16:53:39 INFO  Task:626 -  Using ResourceCalculatorProcessTree : [ ]
2026-07-08 16:53:39 INFO  MapTask:769 - Processing split: hdfs://localhost:9000/user/hadoop/skew-input/transactions.csv:0+92818
2026-07-08 16:53:39 INFO  MapTask:1220 - (EQUATOR) 0 kvi 26214396(104857584)
2026-07-08 16:53:39 INFO  MapTask:1013 - mapreduce.task.io.sort.mb: 100
2026-07-08 16:53:39 INFO  MapTask:1014 - soft limit at 83886080
2026-07-08 16:53:39 INFO  MapTask:1015 - bufstart = 0; bufvoid = 104857600
2026-07-08 16:53:39 INFO  MapTask:1016 - kvstart = 26214396; length = 6553600
2026-07-08 16:53:39 INFO  MapTask:410 - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
2026-07-08 16:53:40 INFO  LocalJobRunner:634 -
2026-07-08 16:53:40 INFO  MapTask:1477 - Starting flush of map output
2026-07-08 16:53:40 INFO  MapTask:1499 - Spilling map output
2026-07-08 16:53:40 INFO  MapTask:1500 - bufstart = 0; bufend = 100500; bufvoid = 104857600
2026-07-08 16:53:40 INFO  MapTask:1502 - kvstart = 26214396(104857584); kvend = 26190400(104761600); length = 23997/6553600
2026-07-08 16:53:40 INFO  MapTask:1700 - Finished spill 0
2026-07-08 16:53:40 INFO  Task:1244 - Task:attempt_local1908886399_0001_m_000000_0 is done. And is in the process of committing
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - map
2026-07-08 16:53:40 INFO  Task:1380 - Task 'attempt_local1908886399_0001_m_000000_0' done.
2026-07-08 16:53:40 INFO  Task:1276 - Final Counters for attempt_local1908886399_0001_m_000000_0: Counters: 23
        File System Counters
                FILE: Number of bytes read=25793
                FILE: Number of bytes written=777299
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=92818
                HDFS: Number of bytes written=0
                HDFS: Number of read operations=5
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=1
                HDFS: Number of bytes read erasure-coded=0
        Map-Reduce Framework
                Map input records=6000
                Map output records=6000
                Map output bytes=100500
                Map output materialized bytes=112524
                Input split bytes=126
                Combine input records=0
                Spilled Records=6000
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=9
                Total committed heap usage (bytes)=376438784
        File Input Format Counters
                Bytes Read=92818
2026-07-08 16:53:40 INFO  LocalJobRunner:277 - Finishing task: attempt_local1908886399_0001_m_000000_0
2026-07-08 16:53:40 INFO  LocalJobRunner:486 - map task executor complete.
2026-07-08 16:53:40 INFO  LocalJobRunner:478 - Waiting for reduce tasks
2026-07-08 16:53:40 INFO  LocalJobRunner:330 - Starting task: attempt_local1908886399_0001_r_000000_0
2026-07-08 16:53:40 INFO  PathOutputCommitterFactory:174 - No output committer factory defined, defaulting to FileOutputCommitterFactory
2026-07-08 16:53:40 INFO  FileOutputCommitter:142 - File Output Committer Algorithm version is 2
2026-07-08 16:53:40 INFO  FileOutputCommitter:157 - FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2026-07-08 16:53:40 INFO  Task:626 -  Using ResourceCalculatorProcessTree : [ ]
2026-07-08 16:53:40 INFO  ReduceTask:363 - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@56ded4df
2026-07-08 16:53:40 WARN  MetricsSystemImpl:151 - JobTracker metrics system already initialized!
2026-07-08 16:53:40 INFO  MergeManagerImpl:208 - MergerManager: memoryLimit=1232024320, maxSingleShuffleLimit=308006080, mergeThreshold=813136064, ioSortFactor=10, memToMemMergeOutputsThreshold=10
2026-07-08 16:53:40 INFO  EventFetcher:61 - attempt_local1908886399_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
2026-07-08 16:53:40 INFO  LocalFetcher:147 - localfetcher#1 about to shuffle output of map attempt_local1908886399_0001_m_000000_0 decomp: 3502 len: 3506 to MEMORY
2026-07-08 16:53:40 INFO  InMemoryMapOutput:94 - Read 3502 bytes from map-output for attempt_local1908886399_0001_m_000000_0
2026-07-08 16:53:40 INFO  MergeManagerImpl:323 - closeInMemoryFile -> map-output of size: 3502, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->3502
2026-07-08 16:53:40 INFO  EventFetcher:76 - EventFetcher is interrupted.. Returning
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  MergeManagerImpl:699 - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
2026-07-08 16:53:40 INFO  Merger:607 - Merging 1 sorted segments
2026-07-08 16:53:40 INFO  Merger:706 - Down to the last merge-pass, with 1 segments left of total size: 3492 bytes
2026-07-08 16:53:40 INFO  MergeManagerImpl:768 - Merged 1 segments, 3502 bytes to disk to satisfy reduce memory limit
2026-07-08 16:53:40 INFO  MergeManagerImpl:798 - Merging 1 files, 3506 bytes from disk
2026-07-08 16:53:40 INFO  MergeManagerImpl:813 - Merging 0 segments, 0 bytes from memory into reduce
2026-07-08 16:53:40 INFO  Merger:607 - Merging 1 sorted segments
2026-07-08 16:53:40 INFO  Merger:706 - Down to the last merge-pass, with 1 segments left of total size: 3492 bytes
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  deprecation:1442 - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
2026-07-08 16:53:40 INFO  Task:1244 - Task:attempt_local1908886399_0001_r_000000_0 is done. And is in the process of committing
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  Task:1421 - Task attempt_local1908886399_0001_r_000000_0 is allowed to commit now
2026-07-08 16:53:40 INFO  FileOutputCommitter:609 - Saved output of task 'attempt_local1908886399_0001_r_000000_0' to hdfs://localhost:9000/user/hadoop/skew-output-baseline
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - reduce > reduce
2026-07-08 16:53:40 INFO  Task:1380 - Task 'attempt_local1908886399_0001_r_000000_0' done.
2026-07-08 16:53:40 INFO  Task:1276 - Final Counters for attempt_local1908886399_0001_r_000000_0: Counters: 30
        File System Counters
                FILE: Number of bytes read=33499
                FILE: Number of bytes written=780805
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=92818
                HDFS: Number of bytes written=106
                HDFS: Number of read operations=10
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=3
                HDFS: Number of bytes read erasure-coded=0
        Map-Reduce Framework
                Combine input records=0
                Combine output records=0
                Reduce input groups=4
                Reduce shuffle bytes=3506
                Reduce input records=200
                Reduce output records=4
                Spilled Records=200
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=0
                Total committed heap usage (bytes)=376438784
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Output Format Counters
                Bytes Written=106
2026-07-08 16:53:40 INFO  LocalJobRunner:353 - Finishing task: attempt_local1908886399_0001_r_000000_0
2026-07-08 16:53:40 INFO  LocalJobRunner:330 - Starting task: attempt_local1908886399_0001_r_000001_0
2026-07-08 16:53:40 INFO  PathOutputCommitterFactory:174 - No output committer factory defined, defaulting to FileOutputCommitterFactory
2026-07-08 16:53:40 INFO  FileOutputCommitter:142 - File Output Committer Algorithm version is 2
2026-07-08 16:53:40 INFO  FileOutputCommitter:157 - FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2026-07-08 16:53:40 INFO  Task:626 -  Using ResourceCalculatorProcessTree : [ ]
2026-07-08 16:53:40 INFO  ReduceTask:363 - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@179ad542
2026-07-08 16:53:40 WARN  MetricsSystemImpl:151 - JobTracker metrics system already initialized!
2026-07-08 16:53:40 INFO  MergeManagerImpl:208 - MergerManager: memoryLimit=1232024320, maxSingleShuffleLimit=308006080, mergeThreshold=813136064, ioSortFactor=10, memToMemMergeOutputsThreshold=10
2026-07-08 16:53:40 INFO  EventFetcher:61 - attempt_local1908886399_0001_r_000001_0 Thread started: EventFetcher for fetching Map Completion Events
2026-07-08 16:53:40 INFO  LocalFetcher:147 - localfetcher#2 about to shuffle output of map attempt_local1908886399_0001_m_000000_0 decomp: 4352 len: 4356 to MEMORY
2026-07-08 16:53:40 INFO  InMemoryMapOutput:94 - Read 4352 bytes from map-output for attempt_local1908886399_0001_m_000000_0
2026-07-08 16:53:40 INFO  MergeManagerImpl:323 - closeInMemoryFile -> map-output of size: 4352, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->4352
2026-07-08 16:53:40 INFO  EventFetcher:76 - EventFetcher is interrupted.. Returning
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  MergeManagerImpl:699 - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
2026-07-08 16:53:40 INFO  Merger:607 - Merging 1 sorted segments
2026-07-08 16:53:40 INFO  Merger:706 - Down to the last merge-pass, with 1 segments left of total size: 4343 bytes
2026-07-08 16:53:40 INFO  MergeManagerImpl:768 - Merged 1 segments, 4352 bytes to disk to satisfy reduce memory limit
2026-07-08 16:53:40 INFO  MergeManagerImpl:798 - Merging 1 files, 4356 bytes from disk
2026-07-08 16:53:40 INFO  MergeManagerImpl:813 - Merging 0 segments, 0 bytes from memory into reduce
2026-07-08 16:53:40 INFO  Merger:607 - Merging 1 sorted segments
2026-07-08 16:53:40 INFO  Merger:706 - Down to the last merge-pass, with 1 segments left of total size: 4343 bytes
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  Task:1244 - Task:attempt_local1908886399_0001_r_000001_0 is done. And is in the process of committing
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  Task:1421 - Task attempt_local1908886399_0001_r_000001_0 is allowed to commit now
2026-07-08 16:53:40 INFO  FileOutputCommitter:609 - Saved output of task 'attempt_local1908886399_0001_r_000001_0' to hdfs://localhost:9000/user/hadoop/skew-output-baseline
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - reduce > reduce
2026-07-08 16:53:40 INFO  Task:1380 - Task 'attempt_local1908886399_0001_r_000001_0' done.
2026-07-08 16:53:40 INFO  Task:1276 - Final Counters for attempt_local1908886399_0001_r_000001_0: Counters: 30
        File System Counters
                FILE: Number of bytes read=46407
                FILE: Number of bytes written=785161
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=92818
                HDFS: Number of bytes written=218
                HDFS: Number of read operations=15
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=5
                HDFS: Number of bytes read erasure-coded=0
        Map-Reduce Framework
                Combine input records=0
                Combine output records=0
                Reduce input groups=5
                Reduce shuffle bytes=4356
                Reduce input records=250
                Reduce output records=5
                Spilled Records=250
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=0
                Total committed heap usage (bytes)=376438784
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Output Format Counters
                Bytes Written=112
2026-07-08 16:53:40 INFO  LocalJobRunner:353 - Finishing task: attempt_local1908886399_0001_r_000001_0
2026-07-08 16:53:40 INFO  LocalJobRunner:330 - Starting task: attempt_local1908886399_0001_r_000002_0
2026-07-08 16:53:40 INFO  PathOutputCommitterFactory:174 - No output committer factory defined, defaulting to FileOutputCommitterFactory
2026-07-08 16:53:40 INFO  FileOutputCommitter:142 - File Output Committer Algorithm version is 2
2026-07-08 16:53:40 INFO  FileOutputCommitter:157 - FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2026-07-08 16:53:40 INFO  Task:626 -  Using ResourceCalculatorProcessTree : [ ]
2026-07-08 16:53:40 INFO  ReduceTask:363 - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@7917e8c8
2026-07-08 16:53:40 WARN  MetricsSystemImpl:151 - JobTracker metrics system already initialized!
2026-07-08 16:53:40 INFO  MergeManagerImpl:208 - MergerManager: memoryLimit=1232024320, maxSingleShuffleLimit=308006080, mergeThreshold=813136064, ioSortFactor=10, memToMemMergeOutputsThreshold=10
2026-07-08 16:53:40 INFO  EventFetcher:61 - attempt_local1908886399_0001_r_000002_0 Thread started: EventFetcher for fetching Map Completion Events
2026-07-08 16:53:40 INFO  LocalFetcher:147 - localfetcher#3 about to shuffle output of map attempt_local1908886399_0001_m_000000_0 decomp: 100252 len: 100256 to MEMORY
2026-07-08 16:53:40 INFO  InMemoryMapOutput:94 - Read 100252 bytes from map-output for attempt_local1908886399_0001_m_000000_0
2026-07-08 16:53:40 INFO  MergeManagerImpl:323 - closeInMemoryFile -> map-output of size: 100252, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->100252
2026-07-08 16:53:40 INFO  EventFetcher:76 - EventFetcher is interrupted.. Returning
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  MergeManagerImpl:699 - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
2026-07-08 16:53:40 INFO  Merger:607 - Merging 1 sorted segments
2026-07-08 16:53:40 INFO  Merger:706 - Down to the last merge-pass, with 1 segments left of total size: 100243 bytes
2026-07-08 16:53:40 INFO  MergeManagerImpl:768 - Merged 1 segments, 100252 bytes to disk to satisfy reduce memory limit
2026-07-08 16:53:40 INFO  MergeManagerImpl:798 - Merging 1 files, 100256 bytes from disk
2026-07-08 16:53:40 INFO  MergeManagerImpl:813 - Merging 0 segments, 0 bytes from memory into reduce
2026-07-08 16:53:40 INFO  Merger:607 - Merging 1 sorted segments
2026-07-08 16:53:40 INFO  Merger:706 - Down to the last merge-pass, with 1 segments left of total size: 100243 bytes
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  Task:1244 - Task:attempt_local1908886399_0001_r_000002_0 is done. And is in the process of committing
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  Task:1421 - Task attempt_local1908886399_0001_r_000002_0 is allowed to commit now
2026-07-08 16:53:40 INFO  FileOutputCommitter:609 - Saved output of task 'attempt_local1908886399_0001_r_000002_0' to hdfs://localhost:9000/user/hadoop/skew-output-baseline
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - reduce > reduce
2026-07-08 16:53:40 INFO  Task:1380 - Task 'attempt_local1908886399_0001_r_000002_0' done.
2026-07-08 16:53:40 INFO  Task:1276 - Final Counters for attempt_local1908886399_0001_r_000002_0: Counters: 30
        File System Counters
                FILE: Number of bytes read=251115
                FILE: Number of bytes written=885417
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=92818
                HDFS: Number of bytes written=386
                HDFS: Number of read operations=20
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=7
                HDFS: Number of bytes read erasure-coded=0
        Map-Reduce Framework
                Combine input records=0
                Combine output records=0
                Reduce input groups=7
                Reduce shuffle bytes=100256
                Reduce input records=5300
                Reduce output records=7
                Spilled Records=5300
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=0
                Total committed heap usage (bytes)=376438784
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Output Format Counters
                Bytes Written=168
2026-07-08 16:53:40 INFO  LocalJobRunner:353 - Finishing task: attempt_local1908886399_0001_r_000002_0
2026-07-08 16:53:40 INFO  LocalJobRunner:330 - Starting task: attempt_local1908886399_0001_r_000003_0
2026-07-08 16:53:40 INFO  PathOutputCommitterFactory:174 - No output committer factory defined, defaulting to FileOutputCommitterFactory
2026-07-08 16:53:40 INFO  FileOutputCommitter:142 - File Output Committer Algorithm version is 2
2026-07-08 16:53:40 INFO  FileOutputCommitter:157 - FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2026-07-08 16:53:40 INFO  Task:626 -  Using ResourceCalculatorProcessTree : [ ]
2026-07-08 16:53:40 INFO  ReduceTask:363 - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@247940bb
2026-07-08 16:53:40 WARN  MetricsSystemImpl:151 - JobTracker metrics system already initialized!
2026-07-08 16:53:40 INFO  MergeManagerImpl:208 - MergerManager: memoryLimit=1232024320, maxSingleShuffleLimit=308006080, mergeThreshold=813136064, ioSortFactor=10, memToMemMergeOutputsThreshold=10
2026-07-08 16:53:40 INFO  EventFetcher:61 - attempt_local1908886399_0001_r_000003_0 Thread started: EventFetcher for fetching Map Completion Events
2026-07-08 16:53:40 INFO  LocalFetcher:147 - localfetcher#4 about to shuffle output of map attempt_local1908886399_0001_m_000000_0 decomp: 4402 len: 4406 to MEMORY
2026-07-08 16:53:40 INFO  InMemoryMapOutput:94 - Read 4402 bytes from map-output for attempt_local1908886399_0001_m_000000_0
2026-07-08 16:53:40 INFO  MergeManagerImpl:323 - closeInMemoryFile -> map-output of size: 4402, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->4402
2026-07-08 16:53:40 INFO  EventFetcher:76 - EventFetcher is interrupted.. Returning
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  MergeManagerImpl:699 - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
2026-07-08 16:53:40 INFO  Merger:607 - Merging 1 sorted segments
2026-07-08 16:53:40 INFO  Merger:706 - Down to the last merge-pass, with 1 segments left of total size: 4392 bytes
2026-07-08 16:53:40 INFO  MergeManagerImpl:768 - Merged 1 segments, 4402 bytes to disk to satisfy reduce memory limit
2026-07-08 16:53:40 INFO  MergeManagerImpl:798 - Merging 1 files, 4406 bytes from disk
2026-07-08 16:53:40 INFO  MergeManagerImpl:813 - Merging 0 segments, 0 bytes from memory into reduce
2026-07-08 16:53:40 INFO  Merger:607 - Merging 1 sorted segments
2026-07-08 16:53:40 INFO  Merger:706 - Down to the last merge-pass, with 1 segments left of total size: 4392 bytes
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  Task:1244 - Task:attempt_local1908886399_0001_r_000003_0 is done. And is in the process of committing
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - 1 / 1 copied.
2026-07-08 16:53:40 INFO  Task:1421 - Task attempt_local1908886399_0001_r_000003_0 is allowed to commit now
2026-07-08 16:53:40 INFO  FileOutputCommitter:609 - Saved output of task 'attempt_local1908886399_0001_r_000003_0' to hdfs://localhost:9000/user/hadoop/skew-output-baseline
2026-07-08 16:53:40 INFO  LocalJobRunner:634 - reduce > reduce
2026-07-08 16:53:40 INFO  Task:1380 - Task 'attempt_local1908886399_0001_r_000003_0' done.
2026-07-08 16:53:40 INFO  Task:1276 - Final Counters for attempt_local1908886399_0001_r_000003_0: Counters: 30
        File System Counters
                FILE: Number of bytes read=260031
                FILE: Number of bytes written=889823
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=92818
                HDFS: Number of bytes written=499
                HDFS: Number of read operations=25
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=9
                HDFS: Number of bytes read erasure-coded=0
        Map-Reduce Framework
                Combine input records=0
                Combine output records=0
                Reduce input groups=5
                Reduce shuffle bytes=4406
                Reduce input records=250
                Reduce output records=5
                Spilled Records=250
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=0
                Total committed heap usage (bytes)=376438784
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Output Format Counters
                Bytes Written=113
2026-07-08 16:53:40 INFO  LocalJobRunner:353 - Finishing task: attempt_local1908886399_0001_r_000003_0
2026-07-08 16:53:40 INFO  LocalJobRunner:486 - reduce task executor complete.
2026-07-08 16:53:40 INFO  Job:1748 - Job job_local1908886399_0001 running in uber mode : false
2026-07-08 16:53:40 INFO  Job:1755 -  map 100% reduce 100%
2026-07-08 16:53:40 INFO  Job:1766 - Job job_local1908886399_0001 completed successfully
2026-07-08 16:53:40 INFO  Job:1773 - Counters: 36
        File System Counters
                FILE: Number of bytes read=616845
                FILE: Number of bytes written=4118505
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=464090
                HDFS: Number of bytes written=1209
                HDFS: Number of read operations=75
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=25
                HDFS: Number of bytes read erasure-coded=0
        Map-Reduce Framework
                Map input records=6000
                Map output records=6000
                Map output bytes=100500
                Map output materialized bytes=112524
                Input split bytes=126
                Combine input records=0
                Combine output records=0
                Reduce input groups=21
                Reduce shuffle bytes=112524
                Reduce input records=6000
                Reduce output records=21
                Spilled Records=12000
                Shuffled Maps =4
                Failed Shuffles=0
                Merged Map outputs=4
                GC time elapsed (ms)=9
                Total committed heap usage (bytes)=1882193920
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=92818
        File Output Format Counters
                Bytes Written=499
bash-4.2$
```
## Fixing the Skew 

### Phase 1: Salt Keys

Mapper:
- Reads customer transactions
- Adds a random salt bucket (0-9)
- Emits salted customer keys

Example:
CUST_HOT_3 -> 1250.50
CUST_HOT_7 -> 980.25

Reducer:
- Calculates partial sums for each salted key

### Phase 2: Remove Salt

Mapper:
- Removes salt suffix

Example:
CUST_HOT_3 -> CUST_HOT

Reducer:
- Combines partial sums into final totals




## Results

Input:
- 6,000 transactions

Phase 1:
- 207 salted intermediate keys

Phase 2:
- 21 final customer totals

Example output:


CUST_HOT 1270743.47


The hot key was distributed across reducers, reducing the impact of data skew.





















