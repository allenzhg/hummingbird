# Hummingbird
This is a compact and flexible data migration tool that supports data migration among Hadoop, HBase, Doris, MinIO, and local storage.

# Build
mvn package -DskipTests

# Configuration
see src/main/resources/config.properties

# run
java -cp Hummingbird-1.0.0-SNAPSHOT.jar com.zenitera.op_hbase_2local
