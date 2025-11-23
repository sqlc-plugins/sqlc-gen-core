//! SQL schema parsing and constraint extraction.
//!
//! This module provides functionality to parse SQL schema files and extract
//! constraint information (primary keys, foreign keys, indexes)

use crate::plugin::{Column, ForeignKey, Identifier, Index, PrimaryKey, Schema, Table};
use sqlparser::ast::{
    ColumnOption, CreateIndex, CreateTable, ObjectName, Statement, TableConstraint,
};
use sqlparser::dialect::dialect_from_str;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::error::Error;

/// Builder for creating a `plugin::Catalog` from SQL schema definitions.
///
/// A `CatalogBuilder` parses SQL DDL statements using a specific SQL dialect
/// (MySQL, PostgreSQL, SQLite, or Generic) and accumulates schema information.
/// Once parsing is complete, the `build` method can be called to produce a
/// `plugin::Catalog` instance.
///
/// # Examples
///
/// ```
/// use sqlc_gen_core::schema::CatalogBuilder;
///
/// let mut builder = CatalogBuilder::new("postgresql");
/// builder.parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)").unwrap();
///
/// let catalog = builder.build();
///
/// // Access schemas from the built catalog
/// for schema in &catalog.schemas {
///     println!("Schema: {}", schema.name);
/// }
/// ```
/// use sqlc_gen_core::schema::CatalogBuilder;
///
/// let mut builder = CatalogBuilder::new("postgresql");
/// builder.parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)").unwrap();
///
/// let catalog = builder.build();
///
/// // Access schemas from the built catalog
/// for schema in &catalog.schemas {
///     println!("Schema: {}", schema.name);
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogBuilder {
    /// SQL dialect used for parsing statements
    ///
    /// Determines how SQL syntax is interpreted during parsing.
    /// Different dialects support different keywords, data types, and syntax features.
    pub dialect: String,

    /// Map of schema names to schema definitions
    ///
    /// The key is the schema name (empty string for default/unnamed schema),
    /// and the value contains all tables within that schema.
    /// Access this directly to iterate over all schemas or look up specific ones.
    pub schemas: HashMap<String, Schema>,
}

impl Default for CatalogBuilder {
    fn default() -> Self {
        Self {
            dialect: "generic".to_string(),
            schemas: HashMap::new(),
        }
    }
}

impl CatalogBuilder {
    /// Create a new empty builder with the specified SQL dialect
    pub fn new(dialect: &str) -> Self {
        Self {
            dialect: dialect.to_string(),
            schemas: HashMap::new(),
        }
    }

    /// Build the `plugin::Catalog` from the parsed schema information.
    pub fn build(self) -> crate::plugin::Catalog {
        crate::plugin::Catalog {
            name: "".to_string(),
            default_schema: "".to_string(),
            comment: "".to_string(),
            schemas: self.schemas.into_values().collect(),
        }
    }

    /// Merges the schemas and tables from another catalog into this builder.
    ///
    /// If a schema from the `other` catalog already exists in the builder, its
    /// contents (tables, enums, etc.) will be merged into the existing schema.
    /// If an item (table, enum, etc.) with the same name already exists within
    /// a schema, it will be ignored to prevent duplicates.
    ///
    /// # Arguments
    ///
    /// * `other` - A `plugin::Catalog` to merge into the builder.
    pub fn merge_catalog(&mut self, other: crate::plugin::Catalog) {
        for other_schema in other.schemas {
            let builder_schema = self
                .schemas
                .entry(other_schema.name.clone())
                .or_insert_with(|| Schema {
                    name: other_schema.name.clone(),
                    ..Default::default()
                });

            let existing_tables: std::collections::HashSet<String> = builder_schema
                .tables
                .iter()
                .filter_map(|t| t.rel.as_ref().map(|r| r.name.clone()))
                .collect();
            for table in other_schema.tables {
                if let Some(rel) = &table.rel {
                    if !existing_tables.contains(&rel.name) {
                        builder_schema.tables.push(table);
                    }
                }
            }

            let existing_enums: std::collections::HashSet<String> = builder_schema
                .enums
                .iter()
                .map(|e| e.name.clone())
                .collect();
            for r#enum in other_schema.enums {
                if !existing_enums.contains(&r#enum.name) {
                    builder_schema.enums.push(r#enum);
                }
            }

            let existing_composites: std::collections::HashSet<String> = builder_schema
                .composite_types
                .iter()
                .map(|c| c.name.clone())
                .collect();
            for composite in other_schema.composite_types {
                if !existing_composites.contains(&composite.name) {
                    builder_schema.composite_types.push(composite);
                }
            }
        }
    }

    /// Parse SQL schema from a string and return a Schema
    pub fn parse_sql(&mut self, sql: &str) -> Result<(), Box<dyn Error>> {
        let dialect =
            dialect_from_str(&self.dialect).ok_or(format!("Unknown dialect: {}", self.dialect))?;
        let statements = Parser::parse_sql(dialect.as_ref(), sql)?;

        for statement in statements {
            match statement {
                Statement::CreateTable(table) => {
                    let table_def = Table::from_create_table(&table);
                    let schema_name = table_def
                        .rel
                        .as_ref()
                        .map(|r| r.schema.clone())
                        .unwrap_or_default();

                    let schema =
                        self.schemas
                            .entry(schema_name.clone())
                            .or_insert_with(|| Schema {
                                name: schema_name.clone(),
                                comment: String::new(),
                                tables: Vec::new(),
                                enums: Vec::new(),
                                composite_types: Vec::new(),
                            });

                    schema.tables.push(table_def);
                }
                Statement::CreateIndex(index) => {
                    let (schema_name, table_name) = parse_qualified_name(&index.table_name);

                    if let Some(schema) = self.schemas.get_mut(&schema_name) {
                        if let Some(table) = schema.tables.iter_mut().find(|t| {
                            if let Some(rel) = &t.rel {
                                rel.name == table_name
                            } else {
                                false
                            }
                        }) {
                            let index_def = Index::from_create_index(&index);
                            table.indexes.push(index_def);
                        }
                    }
                }
                Statement::AlterTable {
                    name, operations, ..
                } => {
                    let (schema_name, table_name) = parse_qualified_name(&name);

                    if let Some(schema) = self.schemas.get_mut(&schema_name) {
                        if let Some(table) = schema.tables.iter_mut().find(|t| {
                            if let Some(rel) = &t.rel {
                                rel.name == table_name
                            } else {
                                false
                            }
                        }) {
                            for operation in operations {
                                if let sqlparser::ast::AlterTableOperation::AddConstraint {
                                    constraint,
                                    ..
                                } = operation
                                {
                                    table.add_constraint(constraint);
                                }
                            }
                        }
                    }
                }
                _ => {
                    // Ignore other statements (CREATE VIEW, INSERT, etc.)
                }
            }
        }

        Ok(())
    }
}

impl Table {
    #[cfg(test)]
    fn new_for_test(name: &str, schema: Option<&str>) -> Self {
        Self {
            rel: Some(Identifier {
                catalog: String::new(),
                schema: schema.unwrap_or("").to_string(),
                name: name.to_string(),
            }),
            comment: String::new(),
            columns: vec![],
            primary_key: None,
            foreign_keys: vec![],
            indexes: vec![],
        }
    }

    /// Create a Table from a CREATE TABLE statement
    pub(crate) fn from_create_table(create_table: &CreateTable) -> Self {
        let name = create_table.name.0.last().unwrap().to_string();
        let schema_name = if create_table.name.0.len() > 1 {
            create_table.name.0[0].to_string()
        } else {
            String::new()
        };

        let mut table = Self {
            rel: Some(Identifier {
                catalog: String::new(),
                schema: schema_name,
                name,
            }),
            comment: String::new(),
            columns: create_table
                .columns
                .iter()
                .map(Column::from_column_def)
                .collect(),
            primary_key: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        };

        // Extract inline column constraints (e.g., column_name PRIMARY KEY)
        for column in &create_table.columns {
            for option in &column.options {
                // Try to create a primary key constraint
                if let Some(pk) = PrimaryKey::from_column_option(column.name.to_string(), option) {
                    table.primary_key = Some(pk);
                }

                // Try to create a foreign key constraint
                if let Some(fk) = ForeignKey::from_column_option(column.name.to_string(), option) {
                    table.foreign_keys.push(fk);
                }
            }
        }

        // Extract table-level constraints
        for constraint in &create_table.constraints {
            table.add_constraint(constraint.clone());
        }

        table
    }

    /// Add a constraint to the table from a TableConstraint
    pub(crate) fn add_constraint(&mut self, constraint: TableConstraint) {
        match constraint {
            pk @ TableConstraint::PrimaryKey { .. } => {
                self.primary_key = Some(PrimaryKey::from_table_constraint(pk));
            }
            fk @ TableConstraint::ForeignKey { .. } => {
                self.foreign_keys
                    .push(ForeignKey::from_table_constraint(fk));
            }
            uq @ TableConstraint::Unique { name: Some(_), .. } => {
                self.indexes.push(Index::from_table_constraint(uq));
            }
            _ => {
                // Ignore unnamed unique constraints and other constraint types
            }
        }
    }

    /// Get the fully qualified table name
    ///
    /// Returns the table name in the format "schema.table" if a schema is specified,
    /// otherwise returns just the table name.
    pub fn qualified_name(&self) -> String {
        if let Some(rel) = &self.rel {
            if !rel.schema.is_empty() {
                format!("{}.{}", rel.schema, rel.name)
            } else {
                rel.name.clone()
            }
        } else {
            String::new()
        }
    }

    /// Check if this table has a primary key defined
    ///
    /// Returns `true` if the table has a primary key constraint, `false` otherwise.
    pub fn has_primary_key(&self) -> bool {
        self.primary_key.is_some()
    }
}

impl Column {
    /// Create column from its definition
    pub(crate) fn from_column_def(column: &sqlparser::ast::ColumnDef) -> Self {
        // Check if column is nullable (NOT NULL or PRIMARY KEY constraint)
        let has_not_null = column
            .options
            .iter()
            .any(|opt| matches!(opt.option, ColumnOption::NotNull));

        let is_primary_key = column.options.iter().any(|opt| {
            matches!(
                opt.option,
                ColumnOption::Unique {
                    is_primary: true,
                    ..
                }
            )
        });

        let not_null = has_not_null || is_primary_key;

        Self {
            name: column.name.to_string(),
            not_null,
            is_array: false,
            comment: String::new(),
            length: 0,
            is_named_param: false,
            is_func_call: false,
            scope: String::new(),
            table: None,
            table_alias: String::new(),
            r#type: Some(Identifier {
                catalog: String::new(),
                schema: String::new(),
                name: column.data_type.to_string(),
            }),
            is_sqlc_slice: false,
            embed_table: None,
            original_name: column.name.to_string(),
            unsigned: false,
            array_dims: 0,
        }
    }
}

impl Index {
    /// Create an Index from a CREATE INDEX statement
    fn from_create_index(create_index: &CreateIndex) -> Self {
        create_index
            .name
            .as_ref()
            .map(|name| Self {
                name: name.to_string(),
                columns: create_index
                    .columns
                    .iter()
                    .map(|col| col.column.to_string())
                    .collect(),
                unique: create_index.unique,
            })
            .unwrap()
    }

    /// Create an Index from a TableConstraint::Unique
    pub(crate) fn from_table_constraint(constraint: TableConstraint) -> Self {
        match constraint {
            TableConstraint::Unique {
                name: Some(index_name),
                columns,
                ..
            } => Self {
                name: index_name.to_string(),
                columns: columns.iter().map(|c| c.to_string()).collect(),
                unique: true,
            },
            TableConstraint::Unique { name: None, .. } => {
                panic!("Cannot create Index from unnamed unique constraint")
            }
            _ => panic!("Expected TableConstraint::Unique, got {constraint:?}"),
        }
    }

    /// Check if this index contains the specified column
    pub fn contains(&self, column_name: &str) -> bool {
        self.columns.iter().any(|col| col == column_name)
    }

    /// Check if this is a single-column unique index on the specified column
    pub fn is_unique_on(&self, column_name: &str) -> bool {
        self.unique && self.columns.len() == 1 && self.columns[0] == column_name
    }
}

impl PrimaryKey {
    /// Create a PrimaryKey from a TableConstraint::PrimaryKey
    pub(crate) fn from_table_constraint(constraint: TableConstraint) -> Self {
        match constraint {
            TableConstraint::PrimaryKey { name, columns, .. } => Self {
                columns: columns.iter().map(|c| c.to_string()).collect(),
                name: name.map(|n| n.to_string()).unwrap_or_default(),
            },
            _ => panic!("Expected TableConstraint::PrimaryKey, got {constraint:?}"),
        }
    }

    /// Create a PrimaryKey from an inline column constraint (e.g., column_name PRIMARY KEY)
    pub(crate) fn from_column_option(
        column_name: String,
        option: &sqlparser::ast::ColumnOptionDef,
    ) -> Option<Self> {
        match &option.option {
            ColumnOption::Unique {
                is_primary: true, ..
            } => Some(Self {
                columns: vec![column_name],
                name: option
                    .name
                    .as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_default(),
            }),
            _ => None,
        }
    }

    /// Check if a column is part of this primary key
    pub fn contains(&self, column_name: &str) -> bool {
        self.columns.iter().any(|col| col == column_name)
    }
}

impl ForeignKey {
    /// Create a ForeignKey from a TableConstraint::ForeignKey
    pub(crate) fn from_table_constraint(constraint: TableConstraint) -> Self {
        match constraint {
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => Self {
                columns: columns.iter().map(|c| c.to_string()).collect(),
                referenced_table: foreign_table.to_string(),
                referenced_columns: referred_columns.iter().map(|c| c.to_string()).collect(),
                name: name.map(|n| n.to_string()).unwrap_or_default(),
                on_delete: on_delete
                    .as_ref()
                    .map(|a| a.to_string())
                    .unwrap_or_default(),
                on_update: on_update
                    .as_ref()
                    .map(|a| a.to_string())
                    .unwrap_or_default(),
            },
            _ => panic!("Expected TableConstraint::ForeignKey, got {constraint:?}"),
        }
    }

    /// Create a ForeignKey from an inline column constraint
    pub(crate) fn from_column_option(
        column_name: String,
        option: &sqlparser::ast::ColumnOptionDef,
    ) -> Option<Self> {
        match &option.option {
            ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => Some(Self {
                columns: vec![column_name],
                referenced_table: foreign_table.to_string(),
                referenced_columns: referred_columns.iter().map(|c| c.to_string()).collect(),
                name: option
                    .name
                    .as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_default(),
                on_delete: on_delete
                    .as_ref()
                    .map(|a| a.to_string())
                    .unwrap_or_default(),
                on_update: on_update
                    .as_ref()
                    .map(|a| a.to_string())
                    .unwrap_or_default(),
            }),
            _ => None,
        }
    }

    /// Check if this foreign key references the specified table
    pub fn references(&self, table_name: &str) -> bool {
        self.referenced_table == table_name
    }

    /// Check if this foreign key contains the specified column
    pub fn contains(&self, column_name: &str) -> bool {
        self.columns.iter().any(|col| col == column_name)
    }
}

/// Parse a qualified name into (schema_name, table_name)
///
/// Returns the schema name (empty string for default schema) and the table name.
/// For example: "public.users" -> ("public", "users"), "users" -> ("", "users")
fn parse_qualified_name(name: &ObjectName) -> (String, String) {
    if name.0.len() > 1 {
        (name.0[0].to_string(), name.0[1].to_string())
    } else {
        (String::new(), name.0[0].to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // CatalogBuilder Tests
    // ============================================================================

    #[test]
    fn test_builder_new() {
        let builder = CatalogBuilder::new("postgresql");
        assert_eq!(builder.dialect, "postgresql");
        assert!(builder.schemas.is_empty());
    }

    #[test]
    fn test_builder_default() {
        let builder = CatalogBuilder::default();
        assert_eq!(builder.dialect, "generic");
        assert!(builder.schemas.is_empty());
    }

    #[test]
    fn test_builder_parse_simple_table() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY)";

        let mut builder = CatalogBuilder::new("generic");
        let result = builder.parse_sql(sql);
        assert!(result.is_ok());

        assert_eq!(builder.schemas.len(), 1);
        let schema = builder.schemas.get("").unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert!(schema
            .tables
            .iter()
            .any(|t| t.rel.as_ref().unwrap().name == "users"));
    }

    #[test]
    fn test_builder_parse_qualified_table() {
        let sql = "CREATE TABLE public.users (id INTEGER PRIMARY KEY)";

        let mut builder = CatalogBuilder::new("postgresql");
        let result = builder.parse_sql(sql);
        assert!(result.is_ok());

        assert_eq!(builder.schemas.len(), 1);
        let schema = builder.schemas.get("public").unwrap();
        assert_eq!(schema.name, "public");
        assert!(schema
            .tables
            .iter()
            .any(|t| t.rel.as_ref().unwrap().name == "users"));
    }

    #[test]
    fn test_builder_parse_multiple_tables() {
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY);
            CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER);
        "#;

        let mut builder = CatalogBuilder::new("generic");
        let result = builder.parse_sql(sql);
        assert!(result.is_ok());

        let schema = builder.schemas.get("").unwrap();
        assert_eq!(schema.tables.len(), 2);
        assert!(schema
            .tables
            .iter()
            .any(|t| t.rel.as_ref().unwrap().name == "users"));
        assert!(schema
            .tables
            .iter()
            .any(|t| t.rel.as_ref().unwrap().name == "posts"));
    }

    #[test]
    fn test_builder_parse_create_index() {
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(255));
            CREATE INDEX idx_email ON users (email);
        "#;

        let mut builder = CatalogBuilder::new("generic");
        let result = builder.parse_sql(sql);
        assert!(result.is_ok());

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        assert_eq!(table.indexes.len(), 1);
        assert_eq!(table.indexes[0].name, "idx_email");
    }

    #[test]
    fn test_builder_parse_alter_table() {
        let sql = r#"
            CREATE TABLE users (id INTEGER, email VARCHAR(255));
            ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id);
        "#;

        let mut builder = CatalogBuilder::new("generic");
        let result = builder.parse_sql(sql);
        assert!(result.is_ok());

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        assert!(table.primary_key.is_some());
        assert_eq!(table.primary_key.as_ref().unwrap().columns, vec!["id"]);
    }

    #[test]
    fn test_builder_clone() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY)";

        let mut builder = CatalogBuilder::new("postgresql");
        builder.parse_sql(sql).unwrap();

        let cloned = builder.clone();
        assert_eq!(builder, cloned);
    }

    #[test]
    fn test_builder_build() {
        let sql = "CREATE TABLE public.users (id INTEGER PRIMARY KEY)";

        let mut builder = CatalogBuilder::new("postgresql");
        builder.parse_sql(sql).unwrap();

        let catalog = builder.build();
        assert_eq!(catalog.schemas.len(), 1);
        assert_eq!(catalog.schemas[0].name, "public");
        assert_eq!(catalog.schemas[0].tables.len(), 1);
    }

    #[test]
    fn test_builder_merge_catalog_disjoint_schemas() {
        let sql = "CREATE TABLE public.users (id int)";
        let mut builder = CatalogBuilder::new("postgresql");
        builder.parse_sql(sql).unwrap();

        let sql = "CREATE TABLE auth.accounts (id int)";
        let mut other_builder = CatalogBuilder::new("postgresql");
        other_builder.parse_sql(sql).unwrap();

        let other_catalog = other_builder.build();
        builder.merge_catalog(other_catalog);

        let final_catalog = builder.build();
        assert_eq!(final_catalog.schemas.len(), 2);
        assert!(final_catalog.schemas.iter().any(|s| s.name == "public"));
        assert!(final_catalog.schemas.iter().any(|s| s.name == "auth"));
    }

    #[test]
    fn test_builder_merge_catalog_into_existing_schema() {
        let sql = "CREATE TABLE users (id int)";
        let mut builder = CatalogBuilder::new("postgresql");
        builder.parse_sql(sql).unwrap();

        let sql = "CREATE TABLE posts (id int)";
        let mut other_builder = CatalogBuilder::new("postgresql");
        other_builder.parse_sql(sql).unwrap();

        let other_catalog = other_builder.build();
        builder.merge_catalog(other_catalog);

        let final_catalog = builder.build();
        assert_eq!(final_catalog.schemas.len(), 1);

        let schema = &final_catalog.schemas[0];
        assert_eq!(schema.tables.len(), 2);

        assert!(schema
            .tables
            .iter()
            .any(|t| t.rel.as_ref().unwrap().name == "users"));
        assert!(schema
            .tables
            .iter()
            .any(|t| t.rel.as_ref().unwrap().name == "posts"));
    }

    #[test]
    fn test_builder_merge_catalog_with_duplicates() {
        let sql = "CREATE TABLE users (id int)";
        let mut builder = CatalogBuilder::new("postgresql");
        builder.parse_sql(sql).unwrap();

        let sql = "CREATE TABLE users (id int, name text); CREATE TABLE posts (id int)";
        let mut other_builder = CatalogBuilder::new("postgresql");
        other_builder.parse_sql(sql).unwrap();

        let other_catalog = other_builder.build();
        builder.merge_catalog(other_catalog);

        let final_catalog = builder.build();
        assert_eq!(final_catalog.schemas.len(), 1);

        let schema = &final_catalog.schemas[0];
        assert_eq!(schema.tables.len(), 2); // Should not add the duplicate 'users' table

        let users_table = schema
            .tables
            .iter()
            .find(|t| t.rel.as_ref().unwrap().name == "users")
            .unwrap();
        // The original table (with 1 column) should be preserved, not the new one (with 2 columns)
        assert_eq!(users_table.columns.len(), 1);
    }

    // ============================================================================
    // Schema Tests
    // ============================================================================

    #[test]
    fn test_schema_default() {
        let schema = Schema::default();
        assert_eq!(schema.name, "");
        assert!(schema.tables.is_empty());
    }

    #[test]
    fn test_schema_with_tables() {
        let mut builder = CatalogBuilder::new("generic");
        let sql = "CREATE TABLE myschema.users (id INTEGER PRIMARY KEY)";
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("myschema").unwrap();
        assert_eq!(schema.name, "myschema");
        assert_eq!(schema.tables.len(), 1);
    }

    #[test]
    fn test_schema_clone() {
        let schema = Schema {
            name: "test".to_string(),
            ..Default::default()
        };

        let cloned = schema.clone();
        assert_eq!(schema, cloned);
    }

    // ============================================================================
    // Table Tests
    // ============================================================================

    #[test]
    fn test_table_qualified_name_with_schema() {
        let table = Table::new_for_test("users", Some("public"));
        assert_eq!(table.qualified_name(), "public.users");
    }

    #[test]
    fn test_table_qualified_name_without_schema() {
        let table = Table::new_for_test("users", None);
        assert_eq!(table.qualified_name(), "users");
    }

    #[test]
    fn test_table_qualified_name_with_empty_schema() {
        let table = Table::new_for_test("users", Some(""));
        assert_eq!(table.qualified_name(), "users");
    }

    #[test]
    fn test_table_has_primary_key_true() {
        let mut table = Table::new_for_test("users", None);
        table.primary_key = Some(PrimaryKey {
            name: String::new(),
            columns: vec!["id".to_string()],
        });
        assert!(table.has_primary_key());
    }

    #[test]
    fn test_table_has_primary_key_false() {
        let table = Table::new_for_test("users", None);
        assert!(!table.has_primary_key());
    }

    #[test]
    fn test_table_from_create_table_simple() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255))";

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        assert_eq!(table.rel.as_ref().unwrap().name, "users");
        assert_eq!(table.rel.as_ref().unwrap().schema, "");
        assert_eq!(table.columns.len(), 2);
        assert!(table.has_primary_key());
    }

    #[test]
    fn test_table_from_create_table_with_schema() {
        let sql = "CREATE TABLE public.users (id INTEGER)";

        let mut builder = CatalogBuilder::new("postgresql");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("public").unwrap();
        let table = &schema.tables[0];

        assert_eq!(table.rel.as_ref().unwrap().name, "users");
        assert_eq!(table.rel.as_ref().unwrap().schema, "public");
    }

    #[test]
    fn test_table_clone() {
        let table = Table::new_for_test("users", None);
        let cloned = table.clone();
        assert_eq!(table, cloned);
    }

    // ============================================================================
    // Column Tests
    // ============================================================================

    #[test]
    fn test_column_nullable_by_default() {
        let sql = "CREATE TABLE users (name VARCHAR(255))";

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        let column = &table.columns[0];

        assert_eq!(column.name, "name");
        assert!(!column.not_null);
    }

    #[test]
    fn test_column_not_null_constraint() {
        let mut builder = CatalogBuilder::new("generic");
        let sql = "CREATE TABLE users (name VARCHAR(255) NOT NULL)";
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        let column = &table.columns[0];

        assert_eq!(column.name, "name");
        assert!(column.not_null);
    }

    #[test]
    fn test_column_primary_key_not_nullable() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY)";

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        let column = &table.columns[0];

        assert_eq!(column.name, "id");
        assert!(column.not_null);
    }

    #[test]
    fn test_column_default_value() {
        let sql = "CREATE TABLE users (status VARCHAR(50) DEFAULT 'active')";

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        let column = &table.columns[0];

        assert_eq!(column.name, "status");
        // Note: default values are not stored in plugin::Column
    }

    #[test]
    fn test_column_data_type() {
        let sql = "CREATE TABLE users (id INTEGER, name VARCHAR(255), created_at TIMESTAMP)";

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];

        assert_eq!(table.columns[0].r#type.as_ref().unwrap().name, "INTEGER");
        assert!(table.columns[1]
            .r#type
            .as_ref()
            .unwrap()
            .name
            .contains("VARCHAR"));
        assert_eq!(table.columns[2].r#type.as_ref().unwrap().name, "TIMESTAMP");
    }

    #[test]
    fn test_column_clone() {
        let column = Column {
            name: "test".to_string(),
            not_null: false,
            is_array: false,
            comment: String::new(),
            length: 0,
            is_named_param: false,
            is_func_call: false,
            scope: String::new(),
            table: None,
            table_alias: String::new(),
            r#type: Some(Identifier {
                catalog: String::new(),
                schema: String::new(),
                name: "INTEGER".to_string(),
            }),
            is_sqlc_slice: false,
            embed_table: None,
            original_name: "test".to_string(),
            unsigned: false,
            array_dims: 0,
        };

        let cloned = column.clone();
        assert_eq!(column, cloned);
    }

    // ============================================================================
    // Index Tests
    // ============================================================================

    #[test]
    fn test_index_from_create_index() {
        let sql = r#"
            CREATE TABLE users (email VARCHAR(255));
            CREATE INDEX idx_email ON users (email);
        "#;

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];

        assert_eq!(table.indexes.len(), 1);
        assert_eq!(table.indexes[0].name, "idx_email");
        assert_eq!(table.indexes[0].columns, vec!["email"]);
        assert!(!table.indexes[0].unique);
    }

    #[test]
    fn test_index_unique() {
        let sql = r#"
            CREATE TABLE users (email VARCHAR(255));
            CREATE UNIQUE INDEX idx_email ON users (email);
        "#;

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];

        assert_eq!(table.indexes.len(), 1);
        assert!(table.indexes[0].unique);
    }

    #[test]
    fn test_index_multi_column() {
        let sql = r#"
            CREATE TABLE users (first_name VARCHAR(255), last_name VARCHAR(255));
            CREATE INDEX idx_name ON users (first_name, last_name);
        "#;

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];

        assert_eq!(table.indexes.len(), 1);
        assert_eq!(table.indexes[0].columns.len(), 2);
        assert_eq!(table.indexes[0].columns, vec!["first_name", "last_name"]);
    }

    #[test]
    fn test_index_contains() {
        let index = Index {
            name: "idx_test".to_string(),
            columns: vec!["col1".to_string(), "col2".to_string()],
            unique: false,
        };

        assert!(index.contains("col1"));
        assert!(index.contains("col2"));
        assert!(!index.contains("col3"));
    }

    #[test]
    fn test_index_is_unique_on_true() {
        let index = Index {
            name: "idx_email".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
        };

        assert!(index.is_unique_on("email"));
    }

    #[test]
    fn test_index_is_unique_on_false_not_unique() {
        let index = Index {
            name: "idx_email".to_string(),
            columns: vec!["email".to_string()],
            unique: false,
        };

        assert!(!index.is_unique_on("email"));
    }

    #[test]
    fn test_index_is_unique_on_false_multi_column() {
        let index = Index {
            name: "idx_name".to_string(),
            columns: vec!["first_name".to_string(), "last_name".to_string()],
            unique: true,
        };

        assert!(!index.is_unique_on("first_name"));
    }

    #[test]
    fn test_index_is_unique_on_false_wrong_column() {
        let index = Index {
            name: "idx_email".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
        };

        assert!(!index.is_unique_on("username"));
    }

    #[test]
    fn test_index_clone() {
        let index = Index {
            name: "idx_test".to_string(),
            columns: vec!["col1".to_string()],
            unique: true,
        };

        let cloned = index.clone();
        assert_eq!(index, cloned);
    }

    // ============================================================================
    // PrimaryKey Tests
    // ============================================================================

    #[test]
    fn test_primary_key_single_column() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY)";

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        let pk = table.primary_key.as_ref().unwrap();

        assert_eq!(pk.columns.len(), 1);
        assert_eq!(pk.columns[0], "id");
    }

    #[test]
    fn test_primary_key_composite() {
        let sql =
            "CREATE TABLE user_roles (user_id INTEGER, role_id INTEGER, PRIMARY KEY (user_id, role_id))";

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        let pk = table.primary_key.as_ref().unwrap();

        assert_eq!(pk.columns.len(), 2);
        assert_eq!(pk.columns, vec!["user_id", "role_id"]);
    }

    #[test]
    fn test_primary_key_named_constraint() {
        let sql = "CREATE TABLE users (id INTEGER, CONSTRAINT pk_users PRIMARY KEY (id))";

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        let pk = table.primary_key.as_ref().unwrap();

        assert_eq!(pk.name, "pk_users");
        assert_eq!(pk.columns, vec!["id"]);
    }

    #[test]
    fn test_primary_key_contains() {
        let pk = PrimaryKey {
            name: String::new(),
            columns: vec!["id".to_string(), "tenant_id".to_string()],
        };

        assert!(pk.contains("id"));
        assert!(pk.contains("tenant_id"));
        assert!(!pk.contains("email"));
    }

    #[test]
    fn test_primary_key_clone() {
        let pk = PrimaryKey {
            name: "pk_users".to_string(),
            columns: vec!["id".to_string()],
        };

        let cloned = pk.clone();
        assert_eq!(pk, cloned);
    }

    // ============================================================================
    // ForeignKey Tests
    // ============================================================================

    #[test]
    fn test_foreign_key_inline_constraint() {
        let sql = "CREATE TABLE posts (id INTEGER, user_id INTEGER REFERENCES users(id))";

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        assert_eq!(table.foreign_keys.len(), 1);
        let fk = &table.foreign_keys[0];

        assert_eq!(fk.columns, vec!["user_id"]);
        assert_eq!(fk.referenced_table, "users");
        assert_eq!(fk.referenced_columns, vec!["id"]);
    }

    #[test]
    fn test_foreign_key_table_constraint() {
        let sql = r#"
            CREATE TABLE posts (
                id INTEGER,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        "#;

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema
            .tables
            .iter()
            .find(|t| t.rel.as_ref().unwrap().name == "posts")
            .unwrap();

        assert_eq!(table.foreign_keys.len(), 1);
        let fk = &table.foreign_keys[0];
        assert_eq!(fk.columns, vec!["user_id"]);
        assert_eq!(fk.referenced_table, "users");
    }

    #[test]
    fn test_foreign_key_named_constraint() {
        let sql = r#"
            CREATE TABLE posts (
                id INTEGER,
                user_id INTEGER,
                CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)
            )
        "#;

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema
            .tables
            .iter()
            .find(|t| t.rel.as_ref().unwrap().name == "posts")
            .unwrap();
        let fk = &table.foreign_keys[0];

        assert!(!fk.name.is_empty());
        assert_eq!(fk.name, "fk_user");
    }

    #[test]
    fn test_foreign_key_on_delete() {
        let sql = r#"
            CREATE TABLE posts (
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE
            )
        "#;

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema
            .tables
            .iter()
            .find(|t| t.rel.as_ref().unwrap().name == "posts")
            .unwrap();
        let fk = &table.foreign_keys[0];

        assert!(!fk.on_delete.is_empty());
        assert!(fk.on_delete.contains("CASCADE"));
    }

    #[test]
    fn test_foreign_key_on_update() {
        let sql = r#"
            CREATE TABLE posts (
                user_id INTEGER REFERENCES users(id) ON UPDATE CASCADE
            )
        "#;

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema
            .tables
            .iter()
            .find(|t| t.rel.as_ref().unwrap().name == "posts")
            .unwrap();
        let fk = &table.foreign_keys[0];

        assert!(!fk.on_update.is_empty());
        assert!(fk.on_update.contains("CASCADE"));
    }

    #[test]
    fn test_foreign_key_composite() {
        let sql = r#"
            CREATE TABLE order_items (
                order_id INTEGER,
                product_id INTEGER,
                FOREIGN KEY (order_id, product_id) REFERENCES orders(id, product_id)
            )
        "#;

        let mut builder = CatalogBuilder::new("generic");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        let table = &schema.tables[0];
        let fk = &table.foreign_keys[0];

        assert_eq!(fk.columns.len(), 2);
        assert_eq!(fk.columns, vec!["order_id", "product_id"]);
        assert_eq!(fk.referenced_columns.len(), 2);
    }

    #[test]
    fn test_foreign_key_references() {
        let fk = ForeignKey {
            name: String::new(),
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: String::new(),
            on_update: String::new(),
        };

        assert!(fk.references("users"));
        assert!(!fk.references("posts"));
    }

    #[test]
    fn test_foreign_key_contains() {
        let fk = ForeignKey {
            name: String::new(),
            columns: vec!["user_id".to_string(), "tenant_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string(), "tenant_id".to_string()],
            on_delete: String::new(),
            on_update: String::new(),
        };

        assert!(fk.contains("user_id"));
        assert!(fk.contains("tenant_id"));
        assert!(!fk.contains("post_id"));
    }

    #[test]
    fn test_foreign_key_clone() {
        let fk = ForeignKey {
            name: "fk_user".to_string(),
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: "CASCADE".to_string(),
            on_update: String::new(),
        };

        let cloned = fk.clone();
        assert_eq!(fk, cloned);
    }

    // ============================================================================
    // Integration Tests
    // ============================================================================

    #[test]
    fn test_complete_schema_parsing() {
        let sql = r#"
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title VARCHAR(255) NOT NULL,
                content TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            
            CREATE INDEX idx_posts_user_id ON posts (user_id);
            CREATE UNIQUE INDEX idx_users_email ON users (email);
        "#;

        let mut builder = CatalogBuilder::new("postgresql");
        builder.parse_sql(sql).unwrap();

        let schema = builder.schemas.get("").unwrap();
        assert_eq!(schema.tables.len(), 2);

        let users_table = &schema
            .tables
            .iter()
            .find(|t| t.rel.as_ref().unwrap().name == "users")
            .unwrap();
        assert_eq!(users_table.columns.len(), 3);
        assert!(users_table.has_primary_key());
        assert_eq!(users_table.indexes.len(), 1);

        let posts_table = &schema
            .tables
            .iter()
            .find(|t| t.rel.as_ref().unwrap().name == "posts")
            .unwrap();
        assert_eq!(posts_table.foreign_keys.len(), 1);
        assert_eq!(posts_table.indexes.len(), 1);
    }

    #[test]
    fn test_multiple_schemas() {
        let sql = r#"
            CREATE TABLE public.users (id INTEGER PRIMARY KEY);
            CREATE TABLE auth.sessions (id INTEGER PRIMARY KEY);
        "#;

        let mut builder = CatalogBuilder::new("postgresql");
        builder.parse_sql(sql).unwrap();

        assert_eq!(builder.schemas.len(), 2);
        assert!(builder.schemas.contains_key("public"));
        assert!(builder.schemas.contains_key("auth"));
    }
}
