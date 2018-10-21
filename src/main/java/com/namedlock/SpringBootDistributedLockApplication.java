package com.namedlock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author tonyzhi from alibaba.com
 */
@SpringBootApplication()
public class SpringBootDistributedLockApplication {


    public static void main(String[] args) {
        SpringApplication.run(SpringBootDistributedLockApplication.class, args);
    }

}
