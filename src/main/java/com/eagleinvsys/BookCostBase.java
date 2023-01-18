package com.eagleinvsys;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class BookCostBase
{
    private BigDecimal sumLTBookCost;
    private BigDecimal sumSTBookCost;
    private BigDecimal sumSTWsDeferral;
    private BigDecimal sumLTWsDeferral;
    private BigDecimal sumLTTaxCost;
    private BigDecimal sumSTTaxCost;
}
