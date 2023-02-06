package com.eagleinvsys.snowparkpoc.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Builder

@Data
public class FinalCSVReport
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
    private String wsQuerySwitch;

    public String getStringValue()
    {
        return entityId + "," + entityName + "," + acctBasis + "," + acctCurrency + "," + reportType + "," +
            reportStartDate + "," + reportEndDate + "," + reportRunDateTime + "," + titleA + ","
            + (sumValueA == null ? "": sumValueA.stripTrailingZeros()) + ","
            + (titleB == null || titleB.equalsIgnoreCase("DUMMY") ? "" : titleB) + ","
            + (sumValueB == null ? "": sumValueB.stripTrailingZeros());
    }

    public String getHeaderString()
    {
        return "ENTITY_ID, ENTITY_NAME, ACCT_BASIS, BASE_CURRENCY, REPORT_TYPE, REPORT_START_DATE, REPORT_END_DATE," +
            "REPORT_RUN_DATE, TITLE_A, SUM_VALUE_A, TITLE_B, SUM_VALUE_B" ;
    }
}
