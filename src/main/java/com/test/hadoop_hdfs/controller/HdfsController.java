package com.test.hadoop_hdfs.controller;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.test.hadoop_hdfs.service.HdfsService;

@RestController
public class HdfsController {

	@Autowired
	private HdfsService hdfsService;

	@GetMapping("/list-files")
    public List<String> listFiles(@RequestParam String path) {
        try {
            // List all files in the specified HDFS directory
            return hdfsService.listFiles(path).collect(Collectors.toList());
        } catch (IOException e) {
            // Handle error and return a user-friendly message
            return List.of("Error listing files: " + e.getMessage());
        }
    }

    @GetMapping("/read-file")
    public String readFile(@RequestParam String filePath) {
        try {
            // Read the content of the specified HDFS file
            return hdfsService.readFile(filePath);
        } catch (IOException e) {
            // Handle error and return a user-friendly message
            return "Error reading file: " + e.getMessage();
        }
    }
}