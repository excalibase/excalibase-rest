package io.github.excalibase.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for all plain model classes: ColumnInfo, TableInfo, ForeignKeyInfo,
 * CDCEvent, CompositeTypeAttribute, CustomEnumTypeInfo, CustomCompositeTypeInfo,
 * ComputedFieldFunction.
 */
class ModelClassesTest {

    // ── ColumnInfo ────────────────────────────────────────────────────────────

    @Test
    void columnInfo_constructorWithArgs_setsAllFields() {
        ColumnInfo col = new ColumnInfo("id", "integer", true, false);
        assertEquals("id", col.getName());
        assertEquals("integer", col.getType());
        assertTrue(col.isPrimaryKey());
        assertFalse(col.isNullable());
    }

    @Test
    void columnInfo_defaultConstructor_fieldsAreNull() {
        ColumnInfo col = new ColumnInfo();
        assertNull(col.getName());
        assertNull(col.getType());
        assertFalse(col.isPrimaryKey());
        assertFalse(col.isNullable());
    }

    @Test
    void columnInfo_setters_updateFields() {
        ColumnInfo col = new ColumnInfo();
        col.setName("email");
        col.setType("varchar");
        col.setPrimaryKey(false);
        col.setNullable(true);
        assertEquals("email", col.getName());
        assertEquals("varchar", col.getType());
        assertFalse(col.isPrimaryKey());
        assertTrue(col.isNullable());
    }

    @Test
    void columnInfo_originalType_nullByDefault() {
        ColumnInfo col = new ColumnInfo("id", "integer", true, false);
        assertNull(col.getOriginalType());
        assertFalse(col.hasOriginalType());
    }

    @Test
    void columnInfo_setOriginalType_worksCorrectly() {
        ColumnInfo col = new ColumnInfo("status", "postgres_enum:status_type", false, true);
        col.setOriginalType("status_type");
        assertEquals("status_type", col.getOriginalType());
        assertTrue(col.hasOriginalType());
    }

    // ── TableInfo ─────────────────────────────────────────────────────────────

    @Test
    void tableInfo_constructorWithThreeArgs_setsAllFields() {
        ColumnInfo col = new ColumnInfo("id", "integer", true, false);
        ForeignKeyInfo fk = new ForeignKeyInfo("customer_id", "customers", "id");
        TableInfo table = new TableInfo("orders", List.of(col), List.of(fk));

        assertEquals("orders", table.getName());
        assertEquals(1, table.getColumns().size());
        assertEquals(1, table.getForeignKeys().size());
        assertFalse(table.isView());
    }

    @Test
    void tableInfo_constructorWithViewFlag_setsIsView() {
        TableInfo table = new TableInfo("my_view", List.of(), List.of(), true);
        assertTrue(table.isView());
    }

    @Test
    void tableInfo_defaultConstructor_emptyCollections() {
        TableInfo table = new TableInfo();
        assertNull(table.getName());
        assertNotNull(table.getColumns());
        assertNotNull(table.getForeignKeys());
    }

    @Test
    void tableInfo_setters_updateFields() {
        TableInfo table = new TableInfo();
        table.setName("users");
        table.setColumns(List.of(new ColumnInfo("id", "integer", true, false)));
        table.setForeignKeys(List.of());
        table.setView(true);

        assertEquals("users", table.getName());
        assertEquals(1, table.getColumns().size());
        assertTrue(table.isView());
    }

    // ── ForeignKeyInfo ────────────────────────────────────────────────────────

    @Test
    void foreignKeyInfo_constructorWithArgs_setsAllFields() {
        ForeignKeyInfo fk = new ForeignKeyInfo("customer_id", "customers", "id");
        assertEquals("customer_id", fk.getColumnName());
        assertEquals("customers", fk.getReferencedTable());
        assertEquals("id", fk.getReferencedColumn());
    }

    @Test
    void foreignKeyInfo_defaultConstructor_fieldsAreNull() {
        ForeignKeyInfo fk = new ForeignKeyInfo();
        assertNull(fk.getColumnName());
        assertNull(fk.getReferencedTable());
        assertNull(fk.getReferencedColumn());
    }

    @Test
    void foreignKeyInfo_setters_updateFields() {
        ForeignKeyInfo fk = new ForeignKeyInfo();
        fk.setColumnName("order_id");
        fk.setReferencedTable("orders");
        fk.setReferencedColumn("id");

        assertEquals("order_id", fk.getColumnName());
        assertEquals("orders", fk.getReferencedTable());
        assertEquals("id", fk.getReferencedColumn());
    }

    // ── CDCEvent ──────────────────────────────────────────────────────────────

    @Test
    void cdcEvent_defaultConstructor_fieldsAreDefault() {
        CDCEvent event = new CDCEvent();
        assertNull(event.getType());
        assertNull(event.getSchema());
        assertNull(event.getTable());
        assertNull(event.getData());
        assertNull(event.getRawMessage());
        assertNull(event.getLsn());
        assertEquals(0L, event.getTimestamp());
        assertEquals(0L, event.getSourceTimestamp());
    }

    @Test
    void cdcEvent_constructorWithArgs_setsAllFields() {
        CDCEvent event = new CDCEvent(
            CDCEvent.Type.INSERT, "public", "users", "{\"id\":1}",
            "raw", "0/1A2B3C4", 1000L, 999L
        );
        assertEquals(CDCEvent.Type.INSERT, event.getType());
        assertEquals("public", event.getSchema());
        assertEquals("users", event.getTable());
        assertEquals("{\"id\":1}", event.getData());
        assertEquals("raw", event.getRawMessage());
        assertEquals("0/1A2B3C4", event.getLsn());
        assertEquals(1000L, event.getTimestamp());
        assertEquals(999L, event.getSourceTimestamp());
    }

    @Test
    void cdcEvent_setters_updateFields() {
        CDCEvent event = new CDCEvent();
        event.setType(CDCEvent.Type.UPDATE);
        event.setSchema("myschema");
        event.setTable("products");
        event.setData("{\"price\":9.99}");
        event.setRawMessage("raw-update");
        event.setLsn("0/DEADBEEF");
        event.setTimestamp(2000L);
        event.setSourceTimestamp(1999L);

        assertEquals(CDCEvent.Type.UPDATE, event.getType());
        assertEquals("myschema", event.getSchema());
        assertEquals("products", event.getTable());
    }

    @Test
    void cdcEvent_toString_containsKeyInfo() {
        CDCEvent event = new CDCEvent(
            CDCEvent.Type.DELETE, "public", "orders", null, null, null, 5000L, 0L
        );
        String str = event.toString();
        assertTrue(str.contains("DELETE"));
        assertTrue(str.contains("public"));
        assertTrue(str.contains("orders"));
        assertTrue(str.contains("5000"));
    }

    @Test
    void cdcEvent_type_allValuesExist() {
        CDCEvent.Type[] types = CDCEvent.Type.values();
        assertEquals(8, types.length);
        // Verify specific ones are present
        assertNotNull(CDCEvent.Type.valueOf("BEGIN"));
        assertNotNull(CDCEvent.Type.valueOf("COMMIT"));
        assertNotNull(CDCEvent.Type.valueOf("INSERT"));
        assertNotNull(CDCEvent.Type.valueOf("UPDATE"));
        assertNotNull(CDCEvent.Type.valueOf("DELETE"));
        assertNotNull(CDCEvent.Type.valueOf("DDL"));
        assertNotNull(CDCEvent.Type.valueOf("TRUNCATE"));
        assertNotNull(CDCEvent.Type.valueOf("HEARTBEAT"));
    }

    // ── CompositeTypeAttribute ────────────────────────────────────────────────

    @Test
    void compositeTypeAttribute_constructorWithArgs_setsAllFields() {
        CompositeTypeAttribute attr = new CompositeTypeAttribute("street", "text", 1);
        assertEquals("street", attr.getName());
        assertEquals("text", attr.getType());
        assertEquals(1, attr.getOrder());
    }

    @Test
    void compositeTypeAttribute_defaultConstructor_fieldsAreDefault() {
        CompositeTypeAttribute attr = new CompositeTypeAttribute();
        assertNull(attr.getName());
        assertNull(attr.getType());
        assertEquals(0, attr.getOrder());
    }

    @Test
    void compositeTypeAttribute_setters_updateFields() {
        CompositeTypeAttribute attr = new CompositeTypeAttribute();
        attr.setName("city");
        attr.setType("varchar");
        attr.setOrder(2);
        assertEquals("city", attr.getName());
        assertEquals("varchar", attr.getType());
        assertEquals(2, attr.getOrder());
    }

    @Test
    void compositeTypeAttribute_equals_sameData_returnsTrue() {
        CompositeTypeAttribute a = new CompositeTypeAttribute("street", "text", 1);
        CompositeTypeAttribute b = new CompositeTypeAttribute("street", "text", 1);
        assertEquals(a, b);
    }

    @Test
    void compositeTypeAttribute_equals_differentData_returnsFalse() {
        CompositeTypeAttribute a = new CompositeTypeAttribute("street", "text", 1);
        CompositeTypeAttribute b = new CompositeTypeAttribute("city", "text", 2);
        assertNotEquals(a, b);
    }

    @Test
    void compositeTypeAttribute_equals_null_returnsFalse() {
        CompositeTypeAttribute a = new CompositeTypeAttribute("street", "text", 1);
        assertNotEquals(a, null);
    }

    @Test
    void compositeTypeAttribute_equals_self_returnsTrue() {
        CompositeTypeAttribute a = new CompositeTypeAttribute("street", "text", 1);
        assertEquals(a, a);
    }

    @Test
    void compositeTypeAttribute_hashCode_equalObjects_sameHash() {
        CompositeTypeAttribute a = new CompositeTypeAttribute("street", "text", 1);
        CompositeTypeAttribute b = new CompositeTypeAttribute("street", "text", 1);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void compositeTypeAttribute_toString_containsFields() {
        CompositeTypeAttribute attr = new CompositeTypeAttribute("street", "text", 1);
        String str = attr.toString();
        assertTrue(str.contains("street"));
        assertTrue(str.contains("text"));
        assertTrue(str.contains("1"));
    }

    // ── CustomEnumTypeInfo ────────────────────────────────────────────────────

    @Test
    void customEnumTypeInfo_constructorWithArgs_setsAllFields() {
        CustomEnumTypeInfo info = new CustomEnumTypeInfo("public", "status", List.of("active", "inactive"));
        assertEquals("public", info.getSchema());
        assertEquals("status", info.getName());
        assertEquals(List.of("active", "inactive"), info.getValues());
    }

    @Test
    void customEnumTypeInfo_defaultConstructor_fieldsAreNull() {
        CustomEnumTypeInfo info = new CustomEnumTypeInfo();
        assertNull(info.getSchema());
        assertNull(info.getName());
        assertNull(info.getValues());
    }

    @Test
    void customEnumTypeInfo_setters_updateFields() {
        CustomEnumTypeInfo info = new CustomEnumTypeInfo();
        info.setSchema("myschema");
        info.setName("priority");
        info.setValues(List.of("low", "medium", "high"));
        assertEquals("myschema", info.getSchema());
        assertEquals("priority", info.getName());
        assertEquals(3, info.getValues().size());
    }

    @Test
    void customEnumTypeInfo_equals_sameData_returnsTrue() {
        CustomEnumTypeInfo a = new CustomEnumTypeInfo("public", "status", List.of("active"));
        CustomEnumTypeInfo b = new CustomEnumTypeInfo("public", "status", List.of("active"));
        assertEquals(a, b);
    }

    @Test
    void customEnumTypeInfo_equals_differentData_returnsFalse() {
        CustomEnumTypeInfo a = new CustomEnumTypeInfo("public", "status", List.of("active"));
        CustomEnumTypeInfo b = new CustomEnumTypeInfo("public", "status", List.of("inactive"));
        assertNotEquals(a, b);
    }

    @Test
    void customEnumTypeInfo_hashCode_equalObjects_sameHash() {
        CustomEnumTypeInfo a = new CustomEnumTypeInfo("public", "status", List.of("active"));
        CustomEnumTypeInfo b = new CustomEnumTypeInfo("public", "status", List.of("active"));
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void customEnumTypeInfo_toString_containsFields() {
        CustomEnumTypeInfo info = new CustomEnumTypeInfo("public", "status", List.of("active", "inactive"));
        String str = info.toString();
        assertTrue(str.contains("public"));
        assertTrue(str.contains("status"));
    }

    // ── CustomCompositeTypeInfo ───────────────────────────────────────────────

    @Test
    void customCompositeTypeInfo_constructorWithArgs_setsAllFields() {
        CompositeTypeAttribute attr = new CompositeTypeAttribute("street", "text", 1);
        CustomCompositeTypeInfo info = new CustomCompositeTypeInfo("public", "address", List.of(attr));
        assertEquals("public", info.getSchema());
        assertEquals("address", info.getName());
        assertEquals(1, info.getAttributes().size());
    }

    @Test
    void customCompositeTypeInfo_defaultConstructor_fieldsAreNull() {
        CustomCompositeTypeInfo info = new CustomCompositeTypeInfo();
        assertNull(info.getSchema());
        assertNull(info.getName());
        assertNull(info.getAttributes());
    }

    @Test
    void customCompositeTypeInfo_setters_updateFields() {
        CustomCompositeTypeInfo info = new CustomCompositeTypeInfo();
        info.setSchema("myschema");
        info.setName("mytype");
        info.setAttributes(List.of(new CompositeTypeAttribute("field1", "text", 1)));
        assertEquals("myschema", info.getSchema());
        assertEquals("mytype", info.getName());
        assertEquals(1, info.getAttributes().size());
    }

    @Test
    void customCompositeTypeInfo_equals_sameData_returnsTrue() {
        CompositeTypeAttribute attr = new CompositeTypeAttribute("x", "int", 1);
        CustomCompositeTypeInfo a = new CustomCompositeTypeInfo("public", "point", List.of(attr));
        CustomCompositeTypeInfo b = new CustomCompositeTypeInfo("public", "point", List.of(attr));
        assertEquals(a, b);
    }

    @Test
    void customCompositeTypeInfo_hashCode_equalObjects_sameHash() {
        CompositeTypeAttribute attr = new CompositeTypeAttribute("x", "int", 1);
        CustomCompositeTypeInfo a = new CustomCompositeTypeInfo("public", "point", List.of(attr));
        CustomCompositeTypeInfo b = new CustomCompositeTypeInfo("public", "point", List.of(attr));
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void customCompositeTypeInfo_toString_containsFields() {
        CustomCompositeTypeInfo info = new CustomCompositeTypeInfo("public", "address", List.of());
        String str = info.toString();
        assertTrue(str.contains("public"));
        assertTrue(str.contains("address"));
    }

    // ── ComputedFieldFunction ─────────────────────────────────────────────────

    @Test
    void computedFieldFunction_constructorWithArgs_setsAllFields() {
        ComputedFieldFunction fn = new ComputedFieldFunction(
            "customer_full_name", "customer", "full_name", "text", "public"
        );
        assertEquals("customer_full_name", fn.getFunctionName());
        assertEquals("customer", fn.getTableName());
        assertEquals("full_name", fn.getFieldName());
        assertEquals("text", fn.getReturnType());
        assertEquals("public", fn.getSchema());
    }

    @Test
    void computedFieldFunction_defaultConstructor_fieldsAreNull() {
        ComputedFieldFunction fn = new ComputedFieldFunction();
        assertNull(fn.getFunctionName());
        assertNull(fn.getTableName());
        assertNull(fn.getFieldName());
        assertNull(fn.getReturnType());
        assertNull(fn.getSchema());
        assertNull(fn.getAdditionalParameters());
        assertNull(fn.getVolatility());
    }

    @Test
    void computedFieldFunction_setters_updateFields() {
        ComputedFieldFunction fn = new ComputedFieldFunction();
        fn.setFunctionName("order_total");
        fn.setTableName("orders");
        fn.setFieldName("total");
        fn.setReturnType("numeric");
        fn.setSchema("billing");
        fn.setVolatility("STABLE");

        assertEquals("order_total", fn.getFunctionName());
        assertEquals("orders", fn.getTableName());
        assertEquals("STABLE", fn.getVolatility());
    }

    @Test
    void computedFieldFunction_additionalParameters_setAndGet() {
        ComputedFieldFunction fn = new ComputedFieldFunction();
        ComputedFieldFunction.FunctionParameter param =
            new ComputedFieldFunction.FunctionParameter("p1", "text", 2);
        fn.setAdditionalParameters(List.of(param));
        assertEquals(1, fn.getAdditionalParameters().size());
    }

    @Test
    void functionParameter_constructorWithArgs_setsAllFields() {
        ComputedFieldFunction.FunctionParameter param =
            new ComputedFieldFunction.FunctionParameter("language", "text", 1);
        assertEquals("language", param.getName());
        assertEquals("text", param.getType());
        assertEquals(1, param.getPosition());
    }

    @Test
    void functionParameter_defaultConstructor_fieldsAreDefault() {
        ComputedFieldFunction.FunctionParameter param = new ComputedFieldFunction.FunctionParameter();
        assertNull(param.getName());
        assertNull(param.getType());
        assertEquals(0, param.getPosition());
    }

    @Test
    void functionParameter_setters_updateFields() {
        ComputedFieldFunction.FunctionParameter param = new ComputedFieldFunction.FunctionParameter();
        param.setName("lang");
        param.setType("varchar");
        param.setPosition(3);
        assertEquals("lang", param.getName());
        assertEquals("varchar", param.getType());
        assertEquals(3, param.getPosition());
    }
}
