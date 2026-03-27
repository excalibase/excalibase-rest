package io.github.excalibase.service;

import org.springframework.util.MultiValueMap;

import java.util.List;
import java.util.Map;

public interface IAggregationService {

    Map<String, Object> getAggregates(String tableName,
                                       MultiValueMap<String, String> filters,
                                       List<String> functions,
                                       List<String> columns);

    List<Map<String, Object>> getInlineAggregates(String tableName,
                                                    String selectParam,
                                                    MultiValueMap<String, String> filters,
                                                    List<String> groupByColumns);
}
