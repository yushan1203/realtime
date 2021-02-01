package com.atguigu.gmall2020.publisher.service;

import java.math.BigDecimal;
import java.util.Map;

public interface ClickHouseService {
    public BigDecimal getOrderAmount(String date);

    public Map getOrderAmountHour(String date);

}
