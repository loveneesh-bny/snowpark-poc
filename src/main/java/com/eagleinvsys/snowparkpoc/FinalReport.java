package com.eagleinvsys.snowparkpoc;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;

@Builder

@Data
public class FinalReport
{
    private String entityId;
    private String acctBasis;
    private String acctCurrency;
    private String reportType;
    private LocalDate reportRunDate;
    private String titleA;
    private BigDecimal sumValueA;
    private String titleB;
    private BigDecimal sumValueB;

    // todo - not used
    private String wsQuerySwitch;
    private LocalDate reportStartDate;
    private LocalDate reportEndDate;

    public String getStringValue()
    {
        return entityId + "," + acctBasis + "," + acctCurrency + "," + reportType + "," +
            reportRunDate + "," + titleA + "," + sumValueA + "," + titleB + "," + sumValueB;
    }
}
