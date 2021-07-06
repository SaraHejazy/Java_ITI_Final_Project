package com.iti.springmachinelearning;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class SpringMachineLearningApplication {
    public static void main(String[] args) {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(SpringMachineLearningApplication.class);
        builder.headless(false);
        ConfigurableApplicationContext context = builder.run(args);
    }
}
