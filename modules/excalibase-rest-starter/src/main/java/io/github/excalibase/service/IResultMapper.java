package io.github.excalibase.service;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.model.MappedResult;
import io.github.excalibase.model.TableInfo;

import java.util.List;
import java.util.Map;

public interface IResultMapper {

    MappedResult mapJsonBody(Map<String, Object> singleRow, TableInfo tableInfo);

    MappedResult mapResults(List<Map<String, Object>> rows, CompiledQuery query, TableInfo tableInfo);
}
