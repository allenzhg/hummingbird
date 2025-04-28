# Hummingbird
The Hummingbird is an incredibly efficient and adaptable data migration tool. It stands out due to its compact design, which means it consumes minimal system resources while operating, and its high - level flexibility, enabling it to handle diverse data migration scenarios with ease.
One of the most remarkable features of Hummingbird is its wide - ranging support for different data sources and destinations. It can smoothly perform data migration among various well - known big - data and traditional database systems.
In the realm of big data, it can migrate data between Hadoop, a cornerstone in distributed storage and processing, and HBase, a high - performance, scalable NoSQL database. This allows for seamless data transfer from a large - scale data processing environment to a storage system optimized for real - time access.
When it comes to data warehousing, Hummingbird can transfer data to and from Doris, a high - performance MPP (Massively Parallel Processing) analytics database. This is extremely useful for organizations that need to perform complex analytics on their data.
In the traditional database space, it supports data migration with MySQL, a widely used open - source relational database management system. This is crucial for companies that are transitioning from legacy systems to more modern data architectures.
In addition to database systems, Hummingbird also caters to object storage. It can handle data migration between MinIO, an open - source object storage solution, and other storage systems. This is beneficial for scenarios where data needs to be stored in a more cost - effective and scalable object storage environment.
Furthermore, it provides support for local storage. This means that users can easily move data between their local hard drives or servers and other data platforms, which is handy for development and testing purposes.
Another notable aspect is its ability to deal with uncompressed data. Whether it's migrating large volumes of raw data or data that has been kept in an uncompressed format for specific reasons, Hummingbird can handle it efficiently, ensuring that the data remains intact during the migration process. Overall, Hummingbird is a comprehensive solution for all kinds of data migration requirements.

# Build
mvn package -DskipTests

# Configuration
see src/main/resources/config.properties

# run
java -cp Hummingbird-1.0.0-SNAPSHOT.jar com.zenitera.op_hbase_2local
