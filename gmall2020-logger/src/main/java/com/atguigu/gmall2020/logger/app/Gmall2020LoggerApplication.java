package com.atguigu.gmall2020.logger.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.atguigu.gmall2020.logger.controller")
public class Gmall2020LoggerApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall2020LoggerApplication.class, args);
    }

}
