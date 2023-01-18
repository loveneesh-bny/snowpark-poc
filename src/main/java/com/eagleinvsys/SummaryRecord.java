package com.eagleinvsys;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class SummaryRecord
{
    private String summaryTitle;
    private BigDecimal value;
}
