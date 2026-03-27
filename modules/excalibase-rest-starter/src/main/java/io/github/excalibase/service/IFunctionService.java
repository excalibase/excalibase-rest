package io.github.excalibase.service;

import io.github.excalibase.model.ComputedFieldFunction;

import java.util.List;
import java.util.Map;

public interface IFunctionService {

    Object executeRpc(String functionName, Map<String, Object> parameters, String schema);

    List<ComputedFieldFunction> getComputedFields(String tableName, String schema);

    void invalidateMetadataCache();
}
