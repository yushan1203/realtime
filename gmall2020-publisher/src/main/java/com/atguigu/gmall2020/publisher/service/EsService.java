package com.atguigu.gmall2020.publisher.service;

import java.util.Map;

public interface EsService {
    public Long getDauTotal(String date);

    public Map getDauHour(String date);
}
