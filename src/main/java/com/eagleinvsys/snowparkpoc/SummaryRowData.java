package com.eagleinvsys.snowparkpoc;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Builder
@Data
public class SummaryRowData
{
    private String baseCurrency;
    private String entityId;
    private String title1;
    private BigDecimal title1ShortTermGainBase;
    private BigDecimal title1ShortTermLossBase;
    private BigDecimal title1LongTermGainBase;
    private BigDecimal title1LongTermLossBase;
    private BigDecimal title1ShortTermSaleBase;
    private BigDecimal title1ShortTermTaxCost;
    private BigDecimal title1LongTermSaleBase;
    private BigDecimal title1LongTermTaxCost;

    private String title2;
    private BigDecimal title2ShortTermGainBase;
    private BigDecimal title2ShortTermLossBase;
    private BigDecimal title2LongTermGainBase;
    private BigDecimal title2LongTermLossBase;
    private BigDecimal title2ShortTermSaleBase;
    private BigDecimal title2ShortTermBookCost;
    private BigDecimal title2LongTermSaleBase;
    private BigDecimal title2LongTermBookCost;

    private String title3;
    private BigDecimal title3CurrentPeriodSTDeferralBase;
    private BigDecimal title3CurrentPeriodSTReversalBase;
    private BigDecimal title3CurrentPeriodSTInKindReversalBase;
    private BigDecimal title3CurrentPeriodLTDeferralBase;
    private BigDecimal title3CurrentPeriodLTReversalBase;
    private BigDecimal title3CurrentPeriodLTInKindReversalBase;
}
