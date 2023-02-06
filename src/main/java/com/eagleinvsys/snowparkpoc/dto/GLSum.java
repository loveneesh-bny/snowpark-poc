package com.eagleinvsys.snowparkpoc.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class GLSum
{
    private BigDecimal sumGL;
    private BigDecimal sumGLInKind;
}
