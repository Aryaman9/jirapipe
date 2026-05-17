package com.jirapipe;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class JiraPipeApplication {

    public static void main(String[] args) {
        SpringApplication.run(JiraPipeApplication.class, args);
    }
}
