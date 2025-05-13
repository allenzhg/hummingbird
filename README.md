# Hummingbird
Hummingbird is an extremely efficient and adaptable data migration tool. It stands out for its compact design, which means it consumes very few system resources during operation. At the same time, it is highly flexible and can easily handle various data migration scenarios.

One of the most remarkable features of Hummingbird is its extensive support for different data sources and targets. It can smoothly migrate data between various well-known big data and traditional database systems. In the field of big data, it can migrate data between Hadoop, the cornerstone of distributed storage and processing, and HBase, a high-performance and scalable NoSQL database. This enables data to be seamlessly transferred from a large-scale data processing environment to a storage system optimized for real-time access.

In terms of data warehouses, Hummingbird can perform data transfer with Doris, a high-performance massive parallel processing (MPP) analytical database. This is very useful for organizations that need to conduct complex analysis of their data. In the realm of traditional databases, it supports data migration with MySQL, a widely used open-source relational database management system. This is crucial for companies that are transitioning from traditional systems to more modern data architectures. In addition to database systems, Hummingbird is also applicable to object storage. It can handle data migration between MinIO, an open-source object storage solution, and other storage systems. This is helpful for scenarios where data needs to be stored in a more cost-effective and scalable object storage environment.

In addition, it also supports local storage. This means that users can easily move data between local hard drives or servers and other data platforms, which is very convenient for development and testing purposes. Another notable aspect is its ability to handle compressed data. Whether it is migrating a large amount of raw data or data in multi-layer compressed formats at the field level (supporting ZIP and SNAPPY decompression), Hummingbird can process it efficiently to ensure the integrity of the data during the migration process. Overall, Hummingbird is a comprehensive solution that can meet various data migration requirements.

# Build
mvn package -DskipTests

# Configuration
see src/main/resources/config.properties

# run
java -cp Hummingbird-1.0.0-SNAPSHOT.jar com.zenitera.op_hbase_2local
