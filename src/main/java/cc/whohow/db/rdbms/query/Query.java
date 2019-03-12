package cc.whohow.db.rdbms.query;

import java.util.List;

public interface Query {
    String getSQL();

    List<?> getParameters();
}
