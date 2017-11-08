package com.gzunk.dbperformance;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ApplicationMain {

    @Bean
    CopyCallbackHandler copyCallbackHandler() {
        return new CopyCallbackHandler();
    }

    @Bean
    DatabaseRunner dbRunner() {
        return new DatabaseRunner(copyCallbackHandler());
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext app = SpringApplication.run(ApplicationMain.class, args);
        DatabaseRunner dbrun = (DatabaseRunner)app.getBean("dbRunner");
        dbrun.runQuery();
    }

}
