package cc.whohow.db.rdbms.query;

import cc.whohow.db.Json;
import cc.whohow.db.rdbms.type.JdbcTypeFactory;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NamedQuery implements Query {
    private static final Pattern PARAM = Pattern.compile(":(?<name>[a-zA-Z0-9_/]+)");
    protected String sql;
    protected JsonNode parameters;
    protected List<String> parameterNames;

    public NamedQuery(String sql, JsonNode parameters) {
        Matcher matcher = PARAM.matcher(sql);
        StringBuffer buffer = new StringBuffer();
        List<String> parameterNames = new ArrayList<>();
        while (matcher.find()) {
            matcher.appendReplacement(buffer, "?");
            parameterNames.add(matcher.group("name"));
        }
        matcher.appendTail(buffer);
        this.sql = buffer.toString();
        this.parameters = parameters;
        this.parameterNames = parameterNames;
    }

    public List<String> getParameterNames() {
        return parameterNames;
    }

    @Override
    public String getSQL() {
        return sql;
    }

    @Override
    public List<?> getParameters() {
        List<Object> params = new ArrayList<>(parameterNames.size());
        for (String name : parameterNames) {
            params.add(JdbcTypeFactory.getInstance().fromJSON(Json.get(parameters, name)));
        }
        return params;
    }
}
