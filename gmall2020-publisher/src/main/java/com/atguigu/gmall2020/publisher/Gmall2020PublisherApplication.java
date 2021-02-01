package com.atguigu.gmall2020.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableAutoConfiguration
@MapperScan(basePackages="com.atguigu.gmall2020.publisher.mapper")
public class Gmall2020PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall2020PublisherApplication.class, args);
    }

}
