package cc.whohow.db.rdbms.query;

import java.util.Arrays;
import java.util.List;

public class SimpleQuery implements Query {
    protected String sql;
    protected List<?> parameters;

    public SimpleQuery(String sql, Object... parameters) {
        this.sql = sql;
        this.parameters = Arrays.asList(parameters);
    }

    @Override
    public String getSQL() {
        return sql;
    }

    @Override
    public List<?> getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return sql + "\n" + parameters;
    }
}
