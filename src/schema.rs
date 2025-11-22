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

/// SQL schema builder for extracting constraint information from SQL DDL
pub struct SchemaBuilder {
    dialect: SchemaDialect,
}

impl SchemaBuilder {
    /// Create a new schema builder with the specified dialect
    pub fn new(dialect: SchemaDialect) -> Self {
        Self { dialect }
    }

    /// Parse SQL schema from a string and return a Schema
    pub fn parse_sql(&self, sql: &str) -> Result<SchemaDef, Box<dyn Error>> {
        let mut schema = SchemaDef::new();
        let dialect = self.dialect.to_dialect();
        let statements = Parser::parse_sql(dialect.as_ref(), sql)?;

        for statement in statements {
            match statement {
                Statement::CreateTable(create_table) => {
                    self.parse_create_table(
                        &mut schema,
                        create_table.name,
                        create_table.columns,
                        create_table.constraints,
                    )?;
                }
                Statement::CreateIndex(create_index) => {
                    self.parse_create_index(
                        &mut schema,
                        create_index.name,
                        create_index.table_name,
                        create_index.columns,
                        create_index.unique,
                    )?;
                }
                Statement::AlterTable {
                    name, operations, ..
                } => {
                    self.parse_alter_table(&mut schema, name, operations)?;
                }
                _ => {
                    // Ignore other statements (CREATE VIEW, INSERT, etc.)
                }
            }
        }

        Ok(schema)
    }

    /// Parse schema files from sqlc Settings
    pub fn parse_from_settings(
        &self,
        settings: &crate::plugin::Settings,
    ) -> Result<SchemaDef, Box<dyn Error>> {
        let mut schema = SchemaDef::new();
        for schema_file in &settings.schema {
            // In a real plugin, you'd read the file contents
            // For now, this is a placeholder that plugins would extend
            let file_schema = self.parse_sql(schema_file)?;
            // Merge tables from file_schema into schema
            for table in file_schema.tables() {
                schema.add_table(table.clone());
            }
        }
        Ok(schema)
    }

    /// Parse CREATE TABLE statement
    fn parse_create_table(
        &self,
        schema: &mut SchemaDef,
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

        schema.add_table(table_schema);
        Ok(())
    }

    /// Parse CREATE INDEX statement
    fn parse_create_index(
        &self,
        schema: &mut SchemaDef,
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

            if let Some(table_schema) = schema.table_mut(&table_name_str) {
                table_schema.indexes.push(index);
            }
        }

        Ok(())
    }

    /// Parse ALTER TABLE statement
    fn parse_alter_table(
        &self,
        schema: &mut SchemaDef,
        name: ObjectName,
        operations: Vec<sqlparser::ast::AlterTableOperation>,
    ) -> Result<(), String> {
        let table_name = object_name_to_string(&name);

        for operation in operations {
            if let sqlparser::ast::AlterTableOperation::AddConstraint { constraint, .. } = operation
            {
                if let Some(table_schema) = schema.table_mut(&table_name) {
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

/// Schema definition - pure domain object containing table definitions
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDef {
    tables: HashMap<String, TableDef>,
}

impl SchemaDef {
    /// Create a new empty schema
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Get table definition by name
    pub fn table(&self, name: &str) -> Option<&TableDef> {
        self.tables.get(name)
    }

    /// Get all table definitions
    pub fn tables(&self) -> impl Iterator<Item = &TableDef> {
        self.tables.values()
    }

    /// Add a table to the schema (used by parser)
    pub(crate) fn add_table(&mut self, table: TableDef) {
        self.tables.insert(table.name.clone(), table);
    }

    /// Get mutable reference to a table (used by parser)
    pub(crate) fn table_mut(&mut self, name: &str) -> Option<&mut TableDef> {
        self.tables.get_mut(name)
    }
}

impl Default for SchemaDef {
    fn default() -> Self {
        Self::new()
    }
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

impl IndexDef {
    /// Check if this index contains the specified column
    pub fn contains(&self, column_name: &str) -> bool {
        self.columns.iter().any(|col| col == column_name)
    }

    /// Check if this is a single-column unique index on the specified column
    pub fn is_unique_on(&self, column_name: &str) -> bool {
        self.unique && self.columns.len() == 1 && self.columns[0] == column_name
    }
}

/// Primary key constraint information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryKeyDef {
    /// Optional constraint name
    pub name: Option<String>,
    /// Column names that make up the primary key
    pub columns: Vec<String>,
}

impl PrimaryKeyDef {
    /// Check if a column is part of this primary key
    pub fn contains(&self, column_name: &str) -> bool {
        self.columns.iter().any(|col| col == column_name)
    }
}

/// Foreign key constraint information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyDef {
    /// Optional constraint name
    pub name: Option<String>,
    /// Column names in the source table
    pub columns: Vec<String>,
    /// Referenced table name
    pub referenced_table: String,
    /// Referenced column names
    pub referenced_columns: Vec<String>,
    /// ON DELETE action
    pub on_delete: Option<String>,
    /// ON UPDATE action
    pub on_update: Option<String>,
}

impl ForeignKeyDef {
    /// Check if this foreign key references the specified table
    pub fn references(&self, table_name: &str) -> bool {
        self.referenced_table == table_name
    }

    /// Check if this foreign key contains the specified column
    pub fn contains(&self, column_name: &str) -> bool {
        self.columns.iter().any(|col| col == column_name)
    }
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

        let builder = SchemaBuilder::new(SchemaDialect::PostgreSQL);
        let schema = builder.parse_sql(sql).unwrap();

        let table = schema.table("users").unwrap();
        assert_eq!(table.name, "users");
        assert!(table.primary_key.is_some());
        assert_eq!(table.primary_key.as_ref().unwrap().columns, vec!["id"]);
    }

    #[test]
    fn test_parse_create_table_with_table_pk() {
        let sql = "CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id));";

        let builder = SchemaBuilder::new(SchemaDialect::PostgreSQL);
        let schema = builder.parse_sql(sql).unwrap();

        let table = schema.table("users").unwrap();
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

        let builder = SchemaBuilder::new(SchemaDialect::PostgreSQL);
        let schema = builder.parse_sql(sql).unwrap();

        let table = schema.table("posts").unwrap();
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

        let builder = SchemaBuilder::new(SchemaDialect::PostgreSQL);
        let schema = builder.parse_sql(sql).unwrap();

        let table = schema.table("users").unwrap();
        assert_eq!(table.indexes.len(), 1);
        assert_eq!(table.indexes[0].name, "idx_email");
        assert!(table.indexes[0].unique);
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

        let builder = SchemaBuilder::new(SchemaDialect::PostgreSQL);
        let schema = builder.parse_sql(sql).unwrap();

        let table = schema.table("users").unwrap();
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

    #[test]
    fn test_primary_key_contains() {
        let sql = r#"
            CREATE TABLE users (
                id INTEGER,
                org_id INTEGER,
                PRIMARY KEY (id, org_id)
            );
        "#;

        let builder = SchemaBuilder::new(SchemaDialect::PostgreSQL);
        let schema = builder.parse_sql(sql).unwrap();

        let table = schema.table("users").unwrap();
        let pk = table.primary_key.as_ref().unwrap();

        assert!(pk.contains("id"));
        assert!(pk.contains("org_id"));
        assert!(!pk.contains("name"));
        assert!(!pk.contains("nonexistent"));
    }

    #[test]
    fn test_index_contains() {
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT);
            CREATE INDEX idx_email_name ON users(email, name);
        "#;

        let builder = SchemaBuilder::new(SchemaDialect::PostgreSQL);
        let schema = builder.parse_sql(sql).unwrap();

        let table = schema.table("users").unwrap();
        let index = &table.indexes[0];

        assert!(index.contains("email"));
        assert!(index.contains("name"));
        assert!(!index.contains("id"));
        assert!(!index.contains("nonexistent"));
    }

    #[test]
    fn test_index_is_unique_on() {
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT);
            CREATE UNIQUE INDEX idx_email ON users(email);
            CREATE INDEX idx_name ON users(name);
            CREATE UNIQUE INDEX idx_email_name ON users(email, name);
        "#;

        let builder = SchemaBuilder::new(SchemaDialect::PostgreSQL);
        let schema = builder.parse_sql(sql).unwrap();

        let table = schema.table("users").unwrap();

        // idx_email is unique on single column "email"
        assert!(table.indexes[0].is_unique_on("email"));
        assert!(!table.indexes[0].is_unique_on("name"));

        // idx_name is not unique
        assert!(!table.indexes[1].is_unique_on("name"));

        // idx_email_name is unique but on multiple columns
        assert!(!table.indexes[2].is_unique_on("email"));
        assert!(!table.indexes[2].is_unique_on("name"));
    }

    #[test]
    fn test_foreign_key_references() {
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY);
            CREATE TABLE organizations (id INTEGER PRIMARY KEY);
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                org_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (org_id) REFERENCES organizations(id)
            );
        "#;

        let builder = SchemaBuilder::new(SchemaDialect::PostgreSQL);
        let schema = builder.parse_sql(sql).unwrap();

        let table = schema.table("posts").unwrap();

        assert!(table.foreign_keys[0].references("users"));
        assert!(!table.foreign_keys[0].references("organizations"));

        assert!(table.foreign_keys[1].references("organizations"));
        assert!(!table.foreign_keys[1].references("users"));
    }

    #[test]
    fn test_foreign_key_contains() {
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, org_id INTEGER PRIMARY KEY);
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                user_org_id INTEGER,
                FOREIGN KEY (user_id, user_org_id) REFERENCES users(id, org_id)
            );
        "#;

        let builder = SchemaBuilder::new(SchemaDialect::PostgreSQL);
        let schema = builder.parse_sql(sql).unwrap();

        let table = schema.table("posts").unwrap();
        let fk = &table.foreign_keys[0];

        assert!(fk.contains("user_id"));
        assert!(fk.contains("user_org_id"));
        assert!(!fk.contains("id"));
        assert!(!fk.contains("nonexistent"));
    }
}
