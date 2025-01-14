package io.experiment;

import java.util.List;

public class InputRules {
    private String name;
    private List<Column> cols;
    private List<List<Object>> rows;

    // Getters and setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Column> getCols() {
        return cols;
    }

    public void setCols(List<Column> cols) {
        this.cols = cols;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public void setRows(List<List<Object>> rows) {
        this.rows = rows;
    }

    @Override
    public String toString() {
        return "InputRules{" +
                "name='" + name + '\'' +
                ", cols=" + cols +
                ", rows=" + rows +
                '}';
    }

    public static class Column {
        private String name;
        private String type;

        // Getters and setters
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return "Column{" +
                    "name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }
}