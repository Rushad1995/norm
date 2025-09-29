package norm.analyzer

import norm.model.ColumnModel
import norm.model.ParamModel
import norm.model.SqlModel
import norm.util.toCamelCase
import java.sql.Connection
import java.sql.ParameterMetaData
import java.sql.ResultSetMetaData

/**
 * Can Analyze the Tables and PreparedStatements without executing Queries.
 */
class SqlAnalyzer(private val connection: Connection) {
    private val namedParamsRegex = "(?<!:)(:\\w+)".toRegex() // TODO extract
    private val leftJoinRegex = "(?i)(?:LEFT\\s+JOIN\\s+)(?<tables>\\b\\S+\\b)".toRegex()

    fun sqlModel(namedParamSql: String): SqlModel {

        val paramNames = namedParamsRegex.findAll(namedParamSql).map { it.value }.toList()
        val preparableStatement = namedParamsRegex.replace(namedParamSql, "?")
        val preparedStatement = connection.prepareStatement(preparableStatement)

        val parameterMetaData = preparedStatement.parameterMetaData
        val params = (1..parameterMetaData.parameterCount).map {
            ParamModel(
                paramNames[it - 1].substring(1),
                parameterMetaData.getParameterTypeName(it), // db type
                parameterMetaData.isNullable(it) != ParameterMetaData.parameterNoNulls
            )
        }

        val resultSetMetaData: ResultSetMetaData? = preparedStatement.metaData
        val columns = if (resultSetMetaData != null) { // it is a query
        val leftJoinedTables = leftJoinRegex.findAll(namedParamSql).map { it.groups[1]?.value }.toList()

            (1..resultSetMetaData.columnCount).map { idx ->
                val tableName = resultSetMetaData.getTableName(idx)
                val colName = resultSetMetaData.getColumnName(idx)
                val baseNullable = resultSetMetaData.isNullable(idx) != ResultSetMetaData.columnNoNulls
                val colPattern = Regex("(?i)COALESCE\\s*\\(\\s*.*${colName}.*?,\\s*null\\s*\\)")
                val wrappedInCoalesce = colPattern.containsMatchIn(namedParamSql)

                val isNullable = when {
                    wrappedInCoalesce -> true

                    leftJoinedTables.contains(tableName) -> true

                    else -> baseNullable
                }

                ColumnModel(
                    toCamelCase(colName),
                    resultSetMetaData.getColumnTypeName(idx),
                    colName,
                    isNullable
                )
            }
        } else { // it is a command
            listOf<ColumnModel>()
        }
        return SqlModel(params, columns, preparableStatement)
    }
}
