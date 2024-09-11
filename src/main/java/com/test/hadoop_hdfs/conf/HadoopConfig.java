package com.test.hadoop_hdfs.conf;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

@org.springframework.context.annotation.Configuration
public class HadoopConfig {

    @Value("${hadoop.hdfs.uri}")
    private String hdfsUri;

    @Value("${hadoop.hdfs.user}")
    private String hdfsUser;

    @Bean
    public FileSystem fileSystem() throws IOException, InterruptedException, URISyntaxException {
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", hdfsUser);
        System.setProperty("hadoop.home.dir", "D:\\hadoop-3.3.6");  // Make sure to replace this with your Hadoop home directory path

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsUri);
        configuration.set("dfs.client.use.datanode.hostname", "true");

        // Set the filesystem implementation explicitly for the HDFS scheme
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        return FileSystem.get(new URI(hdfsUri), configuration);
    }

    public void listFilesInDirectory(String directoryPath) throws IOException, InterruptedException, URISyntaxException {
        FileSystem fs = fileSystem();
        Path path = new Path(directoryPath);
        fs.listStatus(path);  // You can list the files or perform further operations here
    }
}
