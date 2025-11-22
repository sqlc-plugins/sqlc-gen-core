//! SQL schema parsing and constraint extraction.
//!
//! This module provides functionality to parse SQL schema files and extract
//! constraint information (primary keys, foreign keys, indexes)

use sqlparser::ast::{ColumnOption, ObjectName, Statement, TableConstraint};
use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::error::Error;

/// SQL dialect type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaDialect {
    MySQL,
    SQLite,
    Generic,
    PostgreSQL,
}

impl SchemaDialect {
    /// Convert to sqlparser dialect
    pub fn to_dialect(&self) -> Box<dyn Dialect> {
        match self {
            SchemaDialect::MySQL => Box::new(MySqlDialect {}),
            SchemaDialect::SQLite => Box::new(SQLiteDialect {}),
            SchemaDialect::Generic => Box::new(GenericDialect {}),
            SchemaDialect::PostgreSQL => Box::new(PostgreSqlDialect {}),
        }
    }
}

/// Schema definition parser for extracting constraint information
pub struct SchemaDef {
    dialect: SchemaDialect,
    tables: HashMap<String, TableDef>,
}

impl SchemaDef {
    /// Create a new schema parser with the specified dialect
    pub fn new(dialect: SchemaDialect) -> Self {
        Self {
            dialect,
            tables: HashMap::new(),
        }
    }

    /// Parse SQL schema from a string
    pub fn parse(&mut self, sql: &str) -> Result<(), Box<dyn Error>> {
        let dialect = self.dialect.to_dialect();
        let statements = Parser::parse_sql(dialect.as_ref(), sql)?;

        for statement in statements {
            match statement {
                Statement::CreateTable(create_table) => {
                    self.parse_create_table(
                        create_table.name,
                        create_table.columns,
                        create_table.constraints,
                    )?;
                }
                Statement::CreateIndex(create_index) => {
                    self.parse_create_index(
                        create_index.name,
                        create_index.table_name,
                        create_index.columns,
                        create_index.unique,
                    )?;
                }
                Statement::AlterTable {
                    name, operations, ..
                } => {
                    self.parse_alter_table(name, operations)?;
                }
                _ => {
                    // Ignore other statements (CREATE VIEW, INSERT, etc.)
                }
            }
        }

        Ok(())
    }

    /// Get table schema by name
    pub fn get_table(&self, name: &str) -> Option<&TableDef> {
        self.tables.get(name)
    }

    /// Get all table schemas
    pub fn tables(&self) -> impl Iterator<Item = &TableDef> {
        self.tables.values()
    }

    /// Parse CREATE TABLE statement
    fn parse_create_table(
        &mut self,
        name: ObjectName,
        columns: Vec<sqlparser::ast::ColumnDef>,
        constraints: Vec<TableConstraint>,
    ) -> Result<(), String> {
        let table_name = object_name_to_string(&name);
        let schema_name = if name.0.len() > 1 {
            Some(name.0[0].to_string())
        } else {
            None
        };

        let mut table_schema = TableDef {
            name: table_name.clone(),
            schema: schema_name,
            columns: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        };

        // Parse column definitions
        for column in &columns {
            let data_type = column.data_type.to_string();

            // Check if column is nullable (NOT NULL or PRIMARY KEY constraint)
            let has_not_null = column
                .options
                .iter()
                .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));

            let is_primary_key = column.options.iter().any(|opt| {
                matches!(
                    opt.option,
                    sqlparser::ast::ColumnOption::Unique {
                        is_primary: true,
                        ..
                    }
                )
            });

            let nullable = !has_not_null && !is_primary_key;

            // Extract default value if present
            let default = column.options.iter().find_map(|opt| {
                if let sqlparser::ast::ColumnOption::Default(expr) = &opt.option {
                    Some(expr.to_string())
                } else {
                    None
                }
            });

            table_schema.columns.push(ColumnDef {
                name: column.name.to_string(),
                data_type,
                nullable,
                default,
            });
        }

        // Extract inline column constraints (e.g., column_name PRIMARY KEY)
        for column in columns {
            for option in &column.options {
                match &option.option {
                    ColumnOption::Unique {
                        is_primary,
                        characteristics: _,
                    } if *is_primary => {
                        table_schema.primary_key = Some(PrimaryKeyDef {
                            columns: vec![column.name.to_string()],
                            name: option.name.as_ref().map(|n| n.to_string()),
                        });
                    }
                    ColumnOption::ForeignKey {
                        foreign_table,
                        referred_columns,
                        on_delete,
                        on_update,
                        ..
                    } => {
                        table_schema.foreign_keys.push(ForeignKeyDef {
                            columns: vec![column.name.to_string()],
                            referenced_table: object_name_to_string(foreign_table),
                            referenced_columns: referred_columns
                                .iter()
                                .map(|c| c.to_string())
                                .collect(),
                            name: option.name.as_ref().map(|n| n.to_string()),
                            on_delete: on_delete.as_ref().map(|a| a.to_string()),
                            on_update: on_update.as_ref().map(|a| a.to_string()),
                        });
                    }
                    _ => {}
                }
            }
        }

        // Extract table-level constraints
        for constraint in constraints {
            match constraint {
                TableConstraint::PrimaryKey { name, columns, .. } => {
                    table_schema.primary_key = Some(PrimaryKeyDef {
                        columns: columns.iter().map(|c| c.to_string()).collect(),
                        name: name.map(|n| n.to_string()),
                    });
                }
                TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    ..
                } => {
                    table_schema.foreign_keys.push(ForeignKeyDef {
                        columns: columns.iter().map(|c| c.to_string()).collect(),
                        referenced_table: object_name_to_string(&foreign_table),
                        referenced_columns: referred_columns
                            .iter()
                            .map(|c| c.to_string())
                            .collect(),
                        name: name.map(|n| n.to_string()),
                        on_delete: on_delete.as_ref().map(|a| a.to_string()),
                        on_update: on_update.as_ref().map(|a| a.to_string()),
                    });
                }
                TableConstraint::Unique {
                    name: Some(index_name),
                    columns,
                    ..
                } => {
                    table_schema.indexes.push(IndexDef {
                        name: index_name.to_string(),
                        columns: columns.iter().map(|c| c.to_string()).collect(),
                        unique: true,
                    });
                }
                TableConstraint::Unique { name: None, .. } => {}
                _ => {}
            }
        }

        self.tables.insert(table_name, table_schema);
        Ok(())
    }

    /// Parse CREATE INDEX statement
    fn parse_create_index(
        &mut self,
        name: Option<ObjectName>,
        table_name: ObjectName,
        indices: Vec<sqlparser::ast::IndexColumn>,
        unique: bool,
    ) -> Result<(), String> {
        let table_name_str = object_name_to_string(&table_name);

        if let Some(index_name) = name {
            let index = IndexDef {
                name: object_name_to_string(&index_name),
                columns: indices
                    .iter()
                    .map(|index| format!("{}", index.column))
                    .collect(),
                unique,
            };

            if let Some(table_schema) = self.tables.get_mut(&table_name_str) {
                table_schema.indexes.push(index);
            }
        }

        Ok(())
    }

    /// Parse ALTER TABLE statement
    fn parse_alter_table(
        &mut self,
        name: ObjectName,
        operations: Vec<sqlparser::ast::AlterTableOperation>,
    ) -> Result<(), String> {
        let table_name = object_name_to_string(&name);

        for operation in operations {
            if let sqlparser::ast::AlterTableOperation::AddConstraint { constraint, .. } = operation
            {
                if let Some(table_schema) = self.tables.get_mut(&table_name) {
                    match constraint {
                        TableConstraint::PrimaryKey { name, columns, .. } => {
                            table_schema.primary_key = Some(PrimaryKeyDef {
                                columns: columns.iter().map(|c| c.to_string()).collect(),
                                name: name.map(|n| n.to_string()),
                            });
                        }
                        TableConstraint::ForeignKey {
                            name,
                            columns,
                            foreign_table,
                            referred_columns,
                            on_delete,
                            on_update,
                            ..
                        } => {
                            table_schema.foreign_keys.push(ForeignKeyDef {
                                columns: columns.iter().map(|c| c.to_string()).collect(),
                                referenced_table: object_name_to_string(&foreign_table),
                                referenced_columns: referred_columns
                                    .iter()
                                    .map(|c| c.to_string())
                                    .collect(),
                                name: name.map(|n| n.to_string()),
                                on_delete: on_delete.as_ref().map(|a| a.to_string()),
                                on_update: on_update.as_ref().map(|a| a.to_string()),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }
}

/// Helper functions to work with sqlc data structures
impl SchemaDef {
    /// Parse schema files from sqlc Settings
    pub fn parse_from_settings(
        &mut self,
        settings: &crate::plugin::Settings,
    ) -> Result<(), Box<dyn Error>> {
        for schema_file in &settings.schema {
            // In a real plugin, you'd read the file contents
            // For now, this is a placeholder that plugins would extend
            self.parse(schema_file)?;
        }
        Ok(())
    }

    /// Check if a column is part of the primary key
    pub fn is_primary_key_column(&self, table_name: &str, column_name: &str) -> bool {
        if let Some(table) = self.get_table(table_name) {
            if let Some(pk) = &table.primary_key {
                return pk.columns.iter().any(|col| col == column_name);
            }
        }
        false
    }

    /// Get foreign keys for a specific column
    pub fn get_column_foreign_keys(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Vec<&ForeignKeyDef> {
        if let Some(table) = self.get_table(table_name) {
            return table
                .foreign_keys
                .iter()
                .filter(|fk| fk.columns.iter().any(|col| col == column_name))
                .collect();
        }
        Vec::new()
    }

    /// Get all foreign keys referencing a specific table
    pub fn get_referencing_foreign_keys(
        &self,
        referenced_table: &str,
    ) -> Vec<(&str, &ForeignKeyDef)> {
        let mut result = Vec::new();
        for (table_name, table_schema) in &self.tables {
            for fk in &table_schema.foreign_keys {
                if fk.referenced_table == referenced_table {
                    result.push((table_name.as_str(), fk));
                }
            }
        }
        result
    }

    /// Check if a column has a unique index
    pub fn has_unique_index(&self, table_name: &str, column_name: &str) -> bool {
        if let Some(table) = self.get_table(table_name) {
            return table
                .indexes
                .iter()
                .any(|idx| idx.unique && idx.columns.len() == 1 && idx.columns[0] == column_name);
        }
        false
    }
}

/// Column definition
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: String,
    /// Whether the column is nullable
    pub nullable: bool,
    /// Default value if specified
    pub default: Option<String>,
}

/// Table definition including constraints
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    /// Table name
    pub name: String,
    /// Schema name (if specified)
    pub schema: Option<String>,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// Primary key constraint
    pub primary_key: Option<PrimaryKeyDef>,
    /// Foreign key constraints
    pub foreign_keys: Vec<ForeignKeyDef>,
    /// Indexes
    pub indexes: Vec<IndexDef>,
}

/// Index information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    /// Index name
    pub name: String,
    /// Column names in the index
    pub columns: Vec<String>,
    /// Whether this is a unique index
    pub unique: bool,
}

/// Primary key constraint information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryKeyDef {
    /// Column names that make up the primary key
    pub columns: Vec<String>,
    /// Optional constraint name
    pub name: Option<String>,
}

/// Foreign key constraint information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyDef {
    /// Column names in the source table
    pub columns: Vec<String>,
    /// Referenced table name
    pub referenced_table: String,
    /// Referenced column names
    pub referenced_columns: Vec<String>,
    /// Optional constraint name
    pub name: Option<String>,
    /// ON DELETE action
    pub on_delete: Option<String>,
    /// ON UPDATE action
    pub on_update: Option<String>,
}

/// Convert ObjectName to a simple string
fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|ident| ident.to_string())
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_table_with_inline_pk() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);";

        let mut parser = SchemaDef::new(SchemaDialect::PostgreSQL);
        parser.parse(sql).unwrap();

        let table = parser.get_table("users").unwrap();
        assert_eq!(table.name, "users");
        assert!(table.primary_key.is_some());
        assert_eq!(table.primary_key.as_ref().unwrap().columns, vec!["id"]);
    }

    #[test]
    fn test_parse_create_table_with_table_pk() {
        let sql = "CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id));";

        let mut parser = SchemaDef::new(SchemaDialect::PostgreSQL);
        parser.parse(sql).unwrap();

        let table = parser.get_table("users").unwrap();
        assert_eq!(table.name, "users");
        assert!(table.primary_key.is_some());
        assert_eq!(table.primary_key.as_ref().unwrap().columns, vec!["id"]);
    }

    #[test]
    fn test_parse_create_table_with_fk() {
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY);
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
        "#;

        let mut parser = SchemaDef::new(SchemaDialect::PostgreSQL);
        parser.parse(sql).unwrap();

        let table = parser.get_table("posts").unwrap();
        assert_eq!(table.foreign_keys.len(), 1);
        assert_eq!(table.foreign_keys[0].columns, vec!["user_id"]);
        assert_eq!(table.foreign_keys[0].referenced_table, "users");
        assert_eq!(table.foreign_keys[0].referenced_columns, vec!["id"]);
    }

    #[test]
    fn test_parse_create_index() {
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);
            CREATE UNIQUE INDEX idx_email ON users(email);
        "#;

        let mut parser = SchemaDef::new(SchemaDialect::PostgreSQL);
        parser.parse(sql).unwrap();

        let table = parser.get_table("users").unwrap();
        assert_eq!(table.indexes.len(), 1);
        assert_eq!(table.indexes[0].name, "idx_email");
        assert!(table.indexes[0].unique);
    }

    #[test]
    fn test_is_primary_key_column() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);";

        let mut parser = SchemaDef::new(SchemaDialect::PostgreSQL);
        parser.parse(sql).unwrap();

        assert!(parser.is_primary_key_column("users", "id"));
        assert!(!parser.is_primary_key_column("users", "name"));
    }

    #[test]
    fn test_get_column_foreign_keys() {
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY);
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
        "#;

        let mut parser = SchemaDef::new(SchemaDialect::PostgreSQL);
        parser.parse(sql).unwrap();

        let fks = parser.get_column_foreign_keys("posts", "user_id");
        assert_eq!(fks.len(), 1);
        assert_eq!(fks[0].referenced_table, "users");
    }

    #[test]
    fn test_get_referencing_foreign_keys() {
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY);
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
        "#;

        let mut parser = SchemaDef::new(SchemaDialect::PostgreSQL);
        parser.parse(sql).unwrap();

        let refs = parser.get_referencing_foreign_keys("users");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].0, "posts");
    }

    #[test]
    fn test_has_unique_index() {
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);
            CREATE UNIQUE INDEX idx_email ON users(email);
        "#;

        let mut parser = SchemaDef::new(SchemaDialect::PostgreSQL);
        parser.parse(sql).unwrap();

        assert!(parser.has_unique_index("users", "email"));
        assert!(!parser.has_unique_index("users", "id"));
    }

    #[test]
    fn test_parse_columns() {
        let sql = r#"
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email VARCHAR(255),
                created_at TIMESTAMP DEFAULT NOW()
            );
        "#;

        let mut parser = SchemaDef::new(SchemaDialect::PostgreSQL);
        parser.parse(sql).unwrap();

        let table = parser.get_table("users").unwrap();
        assert_eq!(table.columns.len(), 4);

        // Check id column
        assert_eq!(table.columns[0].name, "id");
        assert_eq!(table.columns[0].data_type, "INTEGER");
        assert!(!table.columns[0].nullable); // PRIMARY KEY implies NOT NULL

        // Check name column
        assert_eq!(table.columns[1].name, "name");
        assert_eq!(table.columns[1].data_type, "TEXT");
        assert!(!table.columns[1].nullable); // NOT NULL constraint

        // Check email column
        assert_eq!(table.columns[2].name, "email");
        assert_eq!(table.columns[2].data_type, "VARCHAR(255)");
        assert!(table.columns[2].nullable); // No NOT NULL constraint

        // Check created_at column
        assert_eq!(table.columns[3].name, "created_at");
        assert_eq!(table.columns[3].data_type, "TIMESTAMP");
        assert!(table.columns[3].nullable);
        assert!(table.columns[3].default.is_some());
    }
}
