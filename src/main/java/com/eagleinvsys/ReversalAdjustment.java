package com.eagleinvsys;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class ReversalAdjustment
{
    private BigDecimal longTermReversalAdjustment;
    private BigDecimal shortTermReversalAdjustment;
}
