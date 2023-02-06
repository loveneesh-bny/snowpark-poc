package com.eagleinvsys.snowparkpoc.controller;

import com.eagleinvsys.snowparkpoc.dto.FinalSummaryReport;
import com.eagleinvsys.snowparkpoc.service.WashSaleReportService;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class WashSaleReportController
{
    @Autowired
    private WashSaleReportService washSaleReportService;

    @GetMapping(value = "/poc/wash-sale-report", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<FinalSummaryReport>> generateWashSaleReport()
    {
        List<FinalSummaryReport> finalSummaryReport = washSaleReportService.generateWashSaleSummaryReport();
        return ResponseEntity.ok(finalSummaryReport);
    }

}
