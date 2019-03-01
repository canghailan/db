package cc.whohow.db.rdbms;

import java.util.List;

public class JdbcSynchronizeQuery {
    private String name;
    private String dataSource;
    private boolean update;
    private boolean streaming;
    private List<String> with;
    private String sql;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public boolean isUpdate() {
        return update;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public boolean isStreaming() {
        return streaming;
    }

    public void setStreaming(boolean streaming) {
        this.streaming = streaming;
    }

    public List<String> getWith() {
        return with;
    }

    public void setWith(List<String> with) {
        this.with = with;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
