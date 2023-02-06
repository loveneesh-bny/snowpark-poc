package com.eagleinvsys.snowparkpoc.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Builder

@Data
public class FinalSummaryReport
{
    private String entityId;
    private String entityName;
    private String acctBasis;
    private String acctCurrency;
    private String reportType;
    private String reportStartDate;
    private String reportEndDate;
    private String reportRunDateTime;

    private String titleA;
    private BigDecimal sumValueA;
    private String titleB;
    private BigDecimal sumValueB;

    // todo - not used
    @JsonIgnore
    private String wsQuerySwitch;
}
