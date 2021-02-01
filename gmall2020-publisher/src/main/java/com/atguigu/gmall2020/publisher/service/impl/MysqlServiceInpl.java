package com.atguigu.gmall2020.publisher.service.impl;

import com.atguigu.gmall2020.publisher.mapper.TrademarkAmountSumMapper;
import com.atguigu.gmall2020.publisher.service.MysqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
@Service
public class MysqlServiceInpl implements MysqlService {

    @Autowired
    TrademarkAmountSumMapper trademarkAmountSumMapper;
    @Override
    public List<Map> getTrademarkSum(String startDate, String endDate) {
        return trademarkAmountSumMapper.selectTradeSum(startDate,endDate);
    }
}
