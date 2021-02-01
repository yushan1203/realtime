package com.atguigu.gmall2020.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall2020.publisher.service.MysqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class DataVController {
    //路径和参数随便定 ，但是返回值要看datav的需要
    @Autowired
    MysqlService mysqlService;

    @GetMapping("trademark-sum")
    public String trademarkSum(@RequestParam("start_date") String startDate, @RequestParam("end_date") String endDate){
        if(startDate.length()==0||  endDate.length()==0){
            return "参数不能为空！";
        }
        startDate = startDate.replace("_", " ");
        endDate = endDate.replace("_", " ");
        List<Map> trademardSum = mysqlService.getTrademarkSum(startDate,endDate);

        return JSON.toJSONString(trademardSum) ;


    }

}
