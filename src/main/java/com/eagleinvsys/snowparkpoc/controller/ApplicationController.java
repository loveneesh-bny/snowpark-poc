package com.eagleinvsys.snowparkpoc.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class ApplicationController
{
    @GetMapping("/")
    public String index()

    {
        return "Greetings from Spring Boot!";
    }
}
