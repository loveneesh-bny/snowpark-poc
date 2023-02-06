package com.eagleinvsys.snowparkpoc.util;

import com.snowflake.snowpark_java.Session;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class SnowparkUtils
{
    @Value("${snowpark-poc.url}")
    private String url;
    @Value("${snowpark-poc.user}")
    private String user;
    @Value("${snowpark-poc.password}")
    private String password;
    @Value("${snowpark-poc.db}")
    private String db;
    @Value("${snowpark-poc.schema}")
    private String schema;
    @Value("${snowpark-poc.warehouse}")
    private String warehouse;
    @Value("${snowpark-poc.role}")
    private String role;

    public Session getSnowparkSession()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("URL", url);
        properties.put("USER", user);
        properties.put("PASSWORD", password);
        properties.put("ROLE", role);
        properties.put("WAREHOUSE", warehouse);
        properties.put("DB", db);
        properties.put("SCHEMA", schema);

        // Create snowflake session
        return Session.builder().configs(properties).create();
    }

    public void closeSession(Session session)
    {
        // close session
        session.close();
    }
}
