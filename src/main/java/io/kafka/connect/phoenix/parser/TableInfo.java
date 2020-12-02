package io.kafka.connect.phoenix.parser;


import org.apache.kafka.connect.data.Schema;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class TableInfo {
    private final String fullName;
    private final String schema;
    private final String name;
    private final List<Field> fields = new ArrayList<>();

    public TableInfo(String schema, String name) {
        if (name.charAt(0) == '"') {
            name = name.substring(1, name.length() - 1);
        }
        this.name = name.toUpperCase();
        if (schema != null) {
            if (schema.charAt(0) == '"') {
                schema = schema.substring(1, schema.length() - 1);
            }
            this.schema = schema.toUpperCase();
            this.fullName = "\"" + this.schema + "\".\"" + this.name + "\"";
        } else {
            this.schema = null;
            this.fullName = "\"" + this.name + "\"";
        }
    }

    public static TableInfo parse(String fullName) {
        String[] split = fullName.split("\\.");
        if (split.length == 1) {
            return new TableInfo(null, split[0]);
        } else {

            return new TableInfo(split[0], split[1]);
        }
    }

    public static TableInfo parse(String fullName, String tableFields) {
        TableInfo tableInfo = TableInfo.parse(fullName);
        if(tableFields != null) {
            for (String fieldStr : tableFields.split(",")) {
                String[] fieldSplit = fieldStr.split(":");
                if (fieldSplit.length == 1) {
                    tableInfo.addFields(fieldSplit[0].trim(), Types.VARCHAR);
                } else {
                    tableInfo.addFields(fieldSplit[0].trim(), fieldSplit[1].trim());
                }
            }
        }
        return tableInfo;
    }

    public String getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }

    public String getFullName() {
        return fullName;
    }

    public List<Field> getFields() {
        return fields;
    }

    /**
     * @param name name
     * @param type java.sql.Types
     */
    public void addFields(String name, int type) {
        this.fields.add(new Field(name, type));
    }
    /**
     * @param name name
     * @param type java.sql.Types
     */
    public void addFields(String name, String type) {
        switch (type.toUpperCase()) {
            case "VARCHAR":
                addFields(name, Types.VARCHAR);
                break;
            case "BOOLEAN":
                addFields(name, Types.BOOLEAN);
                break;
            case "BINARY":
                addFields(name, Types.BINARY);
                break;
            case "FLOAT":
                addFields(name, Types.FLOAT);
                break;
            case "DOUBLE":
                addFields(name, Types.DOUBLE);
                break;
            case "TINYINT":
                addFields(name, Types.TINYINT);
                break;
            case "SMALLINT":
                addFields(name, Types.SMALLINT);
                break;
            case "INTEGER":
                addFields(name, Types.INTEGER);
                break;
            case "BIGINT":
                addFields(name, Types.BIGINT);
                break;
            default:
                throw new EventParsingException(String.format("field type %s not support", type));

        }
    }
    /**
     * @param name name
     * @param type java.sql.Types
     */
    public void addFields(String name, Schema.Type type) {
        switch (type) {
            case STRING:
                addFields(name, Types.VARCHAR);
                break;
            case BOOLEAN:
                addFields(name, Types.BOOLEAN);
                break;
            case BYTES:
                addFields(name, Types.BINARY);
                break;
            case FLOAT32:
                addFields(name, Types.FLOAT);
                break;
            case FLOAT64:
                addFields(name, Types.DOUBLE);
                break;
            case INT8:
                addFields(name, Types.TINYINT);
                break;
            case INT16:
                addFields(name, Types.SMALLINT);
                break;
            case INT32:
                addFields(name, Types.INTEGER);
                break;
            case INT64:
                addFields(name, Types.BIGINT);
                break;
            default:
                throw new EventParsingException(String.format("field type %s not support", type.name()));

        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableInfo tableInfo = (TableInfo) o;
        return fullName.equals(tableInfo.fullName);
    }

    @Override
    public int hashCode() {
        return fullName.hashCode();
    }

    public static class Field {
        /**
         * name name
         */
        private final String name;
        /**
         * type java.sql.Types
         */
        private final int type;

        public Field(String name, int type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        /**
         * @return java.sql.Types
         */
        public int getType() {
            return type;
        }
    }


}
