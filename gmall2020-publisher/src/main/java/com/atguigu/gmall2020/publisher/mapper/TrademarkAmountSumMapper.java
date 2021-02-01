package com.atguigu.gmall2020.publisher.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface TrademarkAmountSumMapper {
    public List<Map> selectTradeSum(@Param("start_Date") String startDate,@Param("end_Date") String endDate);
}
