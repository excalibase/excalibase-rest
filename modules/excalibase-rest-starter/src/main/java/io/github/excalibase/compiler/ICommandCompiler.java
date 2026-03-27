package io.github.excalibase.compiler;

import io.github.excalibase.model.TableInfo;

import java.util.List;
import java.util.Map;

/**
 * Compiles write commands (INSERT / UPDATE / DELETE) into single SQL statements.
 *
 * <p>Every method returns a {@link CompiledQuery} containing the SQL and bind parameters
 * ready for {@code JdbcTemplate.queryForList(sql, params)}.  All generated statements
 * include a {@code RETURNING *} clause so the controller can return the affected record.
 */
public interface ICommandCompiler {

    /** INSERT INTO "table" (...) VALUES (?) RETURNING * */
    CompiledQuery insert(String table, TableInfo info, Map<String, Object> data);

    /** INSERT INTO "table" (...) VALUES (?),(?),... RETURNING * */
    CompiledQuery bulkInsert(String table, TableInfo info, List<Map<String, Object>> rows);

    /** UPDATE "table" SET col=? WHERE pk=? RETURNING * (full update) */
    CompiledQuery update(String table, TableInfo info, String id, Map<String, Object> data);

    /** UPDATE "table" SET col=? WHERE pk=? RETURNING * (partial update — only supplied columns) */
    CompiledQuery patch(String table, TableInfo info, String id, Map<String, Object> data);

    /** DELETE FROM "table" WHERE pk=? RETURNING * */
    CompiledQuery delete(String table, TableInfo info, String id);

    /** INSERT ... ON CONFLICT ("col1", "col2") DO UPDATE SET ... RETURNING * */
    CompiledQuery upsert(String table, TableInfo info, Map<String, Object> data, List<String> conflictColumns);
}
