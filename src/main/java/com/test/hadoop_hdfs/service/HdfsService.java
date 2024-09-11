package com.test.hadoop_hdfs.service;

import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

@Service
public class HdfsService {

    @Value("${hadoop.hdfs.uri}")
    private String hdfsUri;

    @Value("${hadoop.hdfs.user}")
    private String hdfsUser;

    private FileSystem fileSystem;

    @PostConstruct
    public void init() throws IOException, InterruptedException {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", hdfsUri);
        fileSystem = FileSystem.get(URI.create(hdfsUri), config, hdfsUser);
    }

    // List all files in a given HDFS directory
    public Stream<String> listFiles(String hdfsDirectoryPath) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(hdfsDirectoryPath));
        return Stream.of(fileStatuses)
                     .map(status -> status.getPath().toString());
    }

    // Read the content of a specific file
    public String readFile(String hdfsFilePath) throws IOException {
        Path filePath = new Path(hdfsFilePath);
        if (!fileSystem.exists(filePath)) {
            return "File not found: " + hdfsFilePath;
        }

        try (FSDataInputStream inputStream = fileSystem.open(filePath)) {
            return IOUtils.toString(inputStream, "UTF-8");
        }
    }
}

