package com.eagleinvsys.snowparkpoc;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.TimeZone;

@SpringBootApplication
@Slf4j
public class Application
{
    public static void main(String[] args)  throws UnknownHostException
    {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        ConfigurableApplicationContext run = SpringApplication.run(Application.class, args);
        Environment env = run.getEnvironment();
        String serverPort = env.getProperty("server.port");

        log.info("\n----------------------------------------------------------\n\t" +
                "Application '{}' is running! Access URLs:\n\t" +
                "Local: \t\thttp://127.0.0.1:{}\n\t" +
                "----------------------------------------------------------",
            env.getProperty("spring.application.name"),
            serverPort);
        log.info("Running with Spring profile(s) : {}", Arrays.toString(env.getActiveProfiles()));
    }
}
