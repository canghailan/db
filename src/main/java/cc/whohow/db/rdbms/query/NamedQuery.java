package cc.whohow.db.rdbms.query;

import cc.whohow.db.rdbms.type.JdbcTypeFactory;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NamedQuery extends SimpleQuery {
    private static final Pattern PARAM = Pattern.compile(":(?<name>[a-zA-Z0-9_/]+)");
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
        this.parameterNames = parameterNames;
        this.parameters = new Object[parameterNames.size()];
        for (int i = 0; i < parameterNames.size(); i++) {
            String name = parameterNames.get(i);
            JsonNode value = name.startsWith("/") ? parameters.at(name) : parameters.path(name);
            this.parameters[i] = JdbcTypeFactory.getInstance().fromJSON(value);
        }
    }

    public List<String> getParameterNames() {
        return parameterNames;
    }
}
