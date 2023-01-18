package com.eagleinvsys;

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
