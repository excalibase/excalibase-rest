package io.github.excalibase.service;

import io.github.excalibase.model.TableInfo;

import java.util.List;
import java.util.Map;

public interface IDatabaseSchemaService {

    Map<String, TableInfo> getTableSchema();

    void clearCache();

    void invalidateSchemaCache();

    Map<String, Object> getSchemaCacheStats();

    String getAllowedSchema();

    List<String> getEnumValues(String enumTypeName);

    Map<String, String> getCompositeTypeDefinition(String compositeTypeName);
}
