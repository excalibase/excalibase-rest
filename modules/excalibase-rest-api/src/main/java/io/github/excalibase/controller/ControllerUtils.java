package io.github.excalibase.controller;

import io.github.excalibase.constant.OperatorConstants;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;
import java.util.Map;

/**
 * Shared utility methods for REST API controllers.
 */
final class ControllerUtils {

    private ControllerUtils() {}

    /**
     * Extract filter parameters from query params, stripping control parameters
     * except 'order' which the compiler reads.
     */
    static MultiValueMap<String, String> extractFilters(MultiValueMap<String, String> allParams) {
        LinkedMultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        if (allParams != null) {
            allParams.forEach((key, values) -> {
                if (!OperatorConstants.isControlParameter(key) || "order".equals(key)) {
                    filters.put(key, values);
                }
            });
        }
        return filters;
    }

    // ─── YAML serialization ───────────────────────────────────────────────────

    static String convertToYaml(Map<String, Object> map) {
        StringBuilder yaml = new StringBuilder();
        convertMapToYaml(map, yaml, 0);
        return yaml.toString();
    }

    @SuppressWarnings("unchecked")
    private static void convertMapToYaml(Map<String, Object> map, StringBuilder yaml, int indent) {
        String indentStr = "  ".repeat(indent);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            yaml.append(indentStr).append(entry.getKey()).append(":");
            Object value = entry.getValue();
            if (value instanceof Map) {
                yaml.append("\n");
                convertMapToYaml((Map<String, Object>) value, yaml, indent + 1);
            } else if (value instanceof List) {
                yaml.append("\n");
                convertListToYaml((List<Object>) value, yaml, indent + 1);
            } else {
                yaml.append(" ").append(formatYamlValue(value)).append("\n");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void convertListToYaml(List<Object> list, StringBuilder yaml, int indent) {
        String indentStr = "  ".repeat(indent);
        for (Object item : list) {
            yaml.append(indentStr).append("-");
            if (item instanceof Map) {
                yaml.append("\n");
                convertMapToYaml((Map<String, Object>) item, yaml, indent + 1);
            } else if (item instanceof List) {
                yaml.append("\n");
                convertListToYaml((List<Object>) item, yaml, indent + 1);
            } else {
                yaml.append(" ").append(formatYamlValue(item)).append("\n");
            }
        }
    }

    private static String formatYamlValue(Object value) {
        if (value == null) return "null";
        if (value instanceof String str) {
            if (str.contains(":") || str.contains("#") || str.contains("'") || str.contains("\"")
                    || str.contains("\n") || str.contains("[") || str.contains("]")
                    || str.contains("{") || str.contains("}")) {
                return "\"" + str.replace("\"", "\\\"") + "\"";
            }
            return str;
        }
        return value.toString();
    }
}
