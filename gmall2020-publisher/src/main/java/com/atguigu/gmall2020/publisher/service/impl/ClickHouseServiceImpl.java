package com.atguigu.gmall2020.publisher.service.impl;

import com.atguigu.gmall2020.publisher.mapper.OrderMapper;
import com.atguigu.gmall2020.publisher.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ClickHouseServiceImpl implements ClickHouseService {

    @Autowired
    OrderMapper orderMapper;

    @Override
    public BigDecimal getOrderAmount(String date) {

        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHourMap(date);
        Map orderAmountHourMap = new HashMap();
        for(Map map:mapList){
            orderAmountHourMap.put(map.get("hr"),map.get("am"));
        }
        return orderAmountHourMap;
    }
}
