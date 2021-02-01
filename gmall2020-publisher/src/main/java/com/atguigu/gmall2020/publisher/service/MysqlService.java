package com.atguigu.gmall2020.publisher.service;

import java.util.List;
import java.util.Map;

public interface MysqlService {
    public List<Map> getTrademarkSum(String startDate,String endDate);
}
