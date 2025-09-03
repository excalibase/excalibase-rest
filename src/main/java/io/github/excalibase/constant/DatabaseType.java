/*
 * Copyright 2025 Excalibase Team and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.excalibase.constant;

/**
 * Supported database types for REST API generation.
 */
public enum DatabaseType {
    /**
     * PostgreSQL database type - fully supported
     */
    POSTGRES("postgres"),

    /**
     * MySQL database type - not yet implemented
     */
    MYSQL("mysql"),

    /**
     * Oracle database type - not yet implemented
     */
    ORACLE("oracle"),

    /**
     * Microsoft SQL Server database type - not yet implemented
     */
    SQL_SERVER("sqlserver"),

    /**
     * Amazon DynamoDB database type - not yet implemented
     */
    DYNAMODB("dynamodb");

    /**
     * The internal name for this database type
     */
    private String name;

    private DatabaseType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}