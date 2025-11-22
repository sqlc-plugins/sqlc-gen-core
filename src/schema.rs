//! SQL schema parsing and constraint extraction.
//!
//! This module provides functionality to parse SQL schema files and extract
//! constraint information (primary keys, foreign keys, indexes)

use sqlparser::ast::{
    ColumnOption, CreateIndex, CreateTable, ObjectName, Statement, TableConstraint,
};
use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::str::FromStr;

/// SQL dialect type for parsing database-specific syntax
///
/// Different databases support different SQL syntax, keywords, and data types.
/// This enum specifies which dialect to use when parsing SQL statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DatabaseDialect {
    /// MySQL/MariaDB dialect
    ///
    /// Supports MySQL-specific syntax including backtick identifiers,
    /// MySQL data types, and MySQL-specific keywords.
    MySQL,
    
    /// SQLite dialect
    ///
    /// Supports SQLite-specific syntax and its limited type system.
    SQLite,
    
    /// PostgreSQL dialect
    ///
    /// Supports PostgreSQL-specific syntax including dollar-quoted strings,
    /// arrays, and PostgreSQL-specific data types.
    PostgreSQL,
    
    /// Generic SQL dialect (default)
    ///
    /// A generic SQL parser that supports standard SQL syntax.
    /// Use this when working with multiple databases or when the specific
    /// dialect doesn't matter for your use case.
    #[default]
    Generic,
}

impl DatabaseDialect {
    /// Convert to sqlparser dialect
    pub fn to_dialect(&self) -> Box<dyn Dialect> {
        match self {
            DatabaseDialect::MySQL => Box::new(MySqlDialect {}),
            DatabaseDialect::SQLite => Box::new(SQLiteDialect {}),
            DatabaseDialect::Generic => Box::new(GenericDialect {}),
            DatabaseDialect::PostgreSQL => Box::new(PostgreSqlDialect {}),
        }
    }
}

impl FromStr for DatabaseDialect {
    type Err = ParseDialectError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mysql" | "mariadb" => Ok(DatabaseDialect::MySQL),
            "sqlite" | "sqlite3" => Ok(DatabaseDialect::SQLite),
            "postgres" | "postgresql" | "psql" => Ok(DatabaseDialect::PostgreSQL),
            "generic" => Ok(DatabaseDialect::Generic),
            _ => Err(ParseDialectError::UnknownDialect(s.to_string())),
        }
    }
}

impl fmt::Display for DatabaseDialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseDialect::MySQL => write!(f, "mysql"),
            DatabaseDialect::SQLite => write!(f, "sqlite"),
            DatabaseDialect::PostgreSQL => write!(f, "postgresql"),
            DatabaseDialect::Generic => write!(f, "generic"),
        }
    }
}

/// Error type for parsing database dialect from string
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseDialectError {
    /// Unknown dialect name was provided
    UnknownDialect(String),
}

impl fmt::Display for ParseDialectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseDialectError::UnknownDialect(s) => {
                write!(
                    f,
                    "unknown database dialect: '{}'. Valid options: mysql, sqlite, postgresql, generic",
                    s
                )
            }
        }
    }
}

impl Error for ParseDialectError {}

/// Database type containing parsed SQL schema information
///
/// A `Database` represents a collection of schemas parsed from SQL DDL statements.
/// Each database uses a specific SQL dialect (MySQL, PostgreSQL, SQLite, or Generic)
/// to correctly parse dialect-specific syntax.
///
/// # Examples
///
/// ```
/// use sqlc_gen_core::schema::{Database, DatabaseDialect};
///
/// let mut db = Database::new(DatabaseDialect::PostgreSQL);
/// db.parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)").unwrap();
///
/// // Access schemas directly via the public HashMap
/// for (schema_name, schema) in &db.schemas {
///     println!("Schema: {}", schema_name);
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Database {
    /// SQL dialect used for parsing statements
    ///
    /// Determines how SQL syntax is interpreted during parsing.
    /// Different dialects support different keywords, data types, and syntax features.
    pub dialect: DatabaseDialect,
    
    /// Map of schema names to schema definitions
    ///
    /// The key is the schema name (empty string for default/unnamed schema),
    /// and the value contains all tables within that schema.
    /// Access this directly to iterate over all schemas or look up specific ones.
    pub schemas: HashMap<String, Schema>,
}

impl Database {
    /// Create a new empty database with the specified SQL dialect
    pub fn new(dialect: DatabaseDialect) -> Self {
        Self {
            dialect,
            schemas: HashMap::new(),
        }
    }

    /// Parse SQL schema from a string and return a Schema
    pub fn parse_sql(&mut self, sql: &str) -> Result<(), Box<dyn Error>> {
        let dialect = self.dialect.to_dialect();
        let statements = Parser::parse_sql(dialect.as_ref(), sql)?;

        for statement in statements {
            match statement {
                Statement::CreateTable(table) => {
                    let table_def = Table::from_create_table(&table);
                    let schema_name = table_def.schema.clone().unwrap_or_default();

                    let schema = self.schemas
                        .entry(schema_name)
                        .or_insert_with_key(|name| Schema {
                            name: name.clone(),
                            tables: HashMap::new(),
                        });

                    let table_name = table_def.name.clone();
                    schema.tables.insert(table_name, table_def);
                }
                Statement::CreateIndex(index) => {
                    let (schema_name, table_name) = parse_qualified_name(&index.table_name);

                    if let Some(schema) = self.schemas.get_mut(&schema_name) {
                        if let Some(table) = schema.tables.get_mut(&table_name) {
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
                        if let Some(table) = schema.tables.get_mut(&table_name) {
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

/// Schema definition containing a collection of table definitions
///
/// A `Schema` represents a database schema (namespace) that groups related tables together.
/// In databases that don't support schemas, the default schema has an empty string as its name.
///
/// # Examples
///
/// ```
/// use sqlc_gen_core::schema::{Database, DatabaseDialect};
///
/// let mut db = Database::new(DatabaseDialect::PostgreSQL);
/// db.parse_sql("CREATE SCHEMA public; CREATE TABLE public.users (id INTEGER)").unwrap();
///
/// if let Some(schema) = db.schemas.get("public") {
///     println!("Schema: {}", schema.name);
///     for (table_name, table) in &schema.tables {
///         println!("  Table: {}", table_name);
///     }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Schema {
    /// Schema name
    ///
    /// The name of the schema. For databases without schema support or for the default schema,
    /// this will be an empty string. This field is always consistent with the key used in
    /// the `Database.schemas` HashMap.
    pub name: String,
    
    /// Map of table names to table definitions
    ///
    /// The key is the unqualified table name, and the value contains the full table definition
    /// including columns, constraints, and indexes. Access this directly to iterate over all
    /// tables or look up specific ones.
    pub tables: HashMap<String, Table>,
}

/// Table definition including constraints
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    /// Table name
    pub name: String,
    /// Schema name (if specified)
    pub schema: Option<String>,
    /// Column definitions
    pub columns: Vec<Column>,
    /// Primary key constraint
    pub primary_key: Option<PrimaryKey>,
    /// Foreign key constraints
    pub foreign_keys: Vec<ForeignKey>,
    /// Indexes
    pub indexes: Vec<Index>,
}

impl Table {
    /// Create a Table from a CREATE TABLE statement
    pub(crate) fn from_create_table(create_table: &CreateTable) -> Self {
        let name = create_table.name.0.last().unwrap().to_string();
        let schema = if create_table.name.0.len() > 1 {
            Some(create_table.name.0[0].to_string())
        } else {
            None
        };

        let mut table = Self {
            name,
            schema,
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
    ///
    /// # Examples
    ///
    /// ```
    /// # use sqlc_gen_core::schema::Table;
    /// # let table = Table {
    /// #     name: "users".to_string(),
    /// #     schema: Some("public".to_string()),
    /// #     columns: vec![],
    /// #     primary_key: None,
    /// #     foreign_keys: vec![],
    /// #     indexes: vec![],
    /// # };
    /// assert_eq!(table.qualified_name(), "public.users");
    /// ```
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) if !schema.is_empty() => format!("{}.{}", schema, self.name),
            _ => self.name.clone(),
        }
    }

    /// Check if this table has a primary key defined
    ///
    /// Returns `true` if the table has a primary key constraint, `false` otherwise.
    pub fn has_primary_key(&self) -> bool {
        self.primary_key.is_some()
    }
}

/// Column definition
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// Column name as it appears in the database
    pub name: String,
    
    /// SQL data type as a string (e.g., "INTEGER", "VARCHAR(255)", "TIMESTAMP")
    ///
    /// The exact format depends on the SQL dialect used during parsing.
    pub data_type: String,
    
    /// Whether the column allows NULL values
    ///
    /// This is `false` if the column has a NOT NULL constraint or is part of a PRIMARY KEY.
    pub nullable: bool,
    
    /// Default value expression if specified in the schema
    ///
    /// Contains the SQL expression as a string (e.g., "0", "'default'", "NOW()").
    pub default: Option<String>,
}

impl Column {
    /// Create column from its definition
    pub(crate) fn from_column_def(column: &sqlparser::ast::ColumnDef) -> Self {
        let data_type = column.data_type.to_string();

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

        let nullable = !has_not_null && !is_primary_key;

        // Extract default value if present
        let default = column.options.iter().find_map(|opt| {
            if let ColumnOption::Default(expr) = &opt.option {
                Some(expr.to_string())
            } else {
                None
            }
        });

        Self {
            name: column.name.to_string(),
            data_type,
            nullable,
            default,
        }
    }


}

/// Index information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    /// Index name
    pub name: String,
    /// Column names in the index
    pub columns: Vec<String>,
    /// Whether this is a unique index
    pub unique: bool,
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

/// Primary key constraint information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryKey {
    /// Optional constraint name
    pub name: Option<String>,
    /// Column names that make up the primary key
    pub columns: Vec<String>,
}

impl PrimaryKey {
    /// Create a PrimaryKey from a TableConstraint::PrimaryKey
    pub(crate) fn from_table_constraint(constraint: TableConstraint) -> Self {
        match constraint {
            TableConstraint::PrimaryKey { name, columns, .. } => Self {
                columns: columns.iter().map(|c| c.to_string()).collect(),
                name: name.map(|n| n.to_string()),
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
                name: option.name.as_ref().map(|n| n.to_string()),
            }),
            _ => None,
        }
    }

    /// Check if a column is part of this primary key
    pub fn contains(&self, column_name: &str) -> bool {
        self.columns.iter().any(|col| col == column_name)
    }
}

/// Foreign key constraint information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
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
                name: name.map(|n| n.to_string()),
                on_delete: on_delete.as_ref().map(|a| a.to_string()),
                on_update: on_update.as_ref().map(|a| a.to_string()),
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
                name: option.name.as_ref().map(|n| n.to_string()),
                on_delete: on_delete.as_ref().map(|a| a.to_string()),
                on_update: on_update.as_ref().map(|a| a.to_string()),
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
    // DatabaseDialect Tests
    // ============================================================================

    #[test]
    fn test_database_dialect_default() {
        let dialect = DatabaseDialect::default();
        assert_eq!(dialect, DatabaseDialect::Generic);
    }

    #[test]
    fn test_database_dialect_to_dialect_mysql() {
        let dialect = DatabaseDialect::MySQL;
        let boxed = dialect.to_dialect();
        // Test that it can parse MySQL-specific syntax (backticks)
        let result = Parser::parse_sql(boxed.as_ref(), "CREATE TABLE `users` (id INT)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_database_dialect_to_dialect_sqlite() {
        let dialect = DatabaseDialect::SQLite;
        let boxed = dialect.to_dialect();
        // Test that dialect is created successfully
        let result = Parser::parse_sql(boxed.as_ref(), "CREATE TABLE users (id INTEGER)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_database_dialect_to_dialect_postgresql() {
        let dialect = DatabaseDialect::PostgreSQL;
        let boxed = dialect.to_dialect();
        // Test that dialect is created successfully
        let result = Parser::parse_sql(boxed.as_ref(), "CREATE TABLE users (id INTEGER)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_database_dialect_to_dialect_generic() {
        let dialect = DatabaseDialect::Generic;
        let boxed = dialect.to_dialect();
        // Test that dialect is created successfully
        let result = Parser::parse_sql(boxed.as_ref(), "CREATE TABLE users (id INTEGER)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_database_dialect_equality() {
        assert_eq!(DatabaseDialect::MySQL, DatabaseDialect::MySQL);
        assert_ne!(DatabaseDialect::MySQL, DatabaseDialect::PostgreSQL);
    }

    #[test]
    fn test_database_dialect_clone() {
        let dialect = DatabaseDialect::PostgreSQL;
        let cloned = dialect.clone();
        assert_eq!(dialect, cloned);
    }

    #[test]
    fn test_database_dialect_from_str_mysql() {
        assert_eq!("mysql".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::MySQL);
        assert_eq!("MySQL".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::MySQL);
        assert_eq!("MYSQL".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::MySQL);
        assert_eq!("mariadb".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::MySQL);
        assert_eq!("MariaDB".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::MySQL);
    }

    #[test]
    fn test_database_dialect_from_str_sqlite() {
        assert_eq!("sqlite".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::SQLite);
        assert_eq!("SQLite".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::SQLite);
        assert_eq!("sqlite3".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::SQLite);
        assert_eq!("SQLITE3".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::SQLite);
    }

    #[test]
    fn test_database_dialect_from_str_postgresql() {
        assert_eq!("postgresql".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::PostgreSQL);
        assert_eq!("PostgreSQL".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::PostgreSQL);
        assert_eq!("postgres".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::PostgreSQL);
        assert_eq!("POSTGRES".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::PostgreSQL);
        assert_eq!("psql".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::PostgreSQL);
    }

    #[test]
    fn test_database_dialect_from_str_generic() {
        assert_eq!("generic".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::Generic);
        assert_eq!("Generic".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::Generic);
        assert_eq!("GENERIC".parse::<DatabaseDialect>().unwrap(), DatabaseDialect::Generic);
    }

    #[test]
    fn test_database_dialect_from_str_error() {
        let result = "oracle".parse::<DatabaseDialect>();
        assert!(result.is_err());
        
        let err = result.unwrap_err();
        assert_eq!(err, ParseDialectError::UnknownDialect("oracle".to_string()));
        assert!(err.to_string().contains("oracle"));
        assert!(err.to_string().contains("Valid options"));
    }

    #[test]
    fn test_database_dialect_display_mysql() {
        assert_eq!(DatabaseDialect::MySQL.to_string(), "mysql");
    }

    #[test]
    fn test_database_dialect_display_sqlite() {
        assert_eq!(DatabaseDialect::SQLite.to_string(), "sqlite");
    }

    #[test]
    fn test_database_dialect_display_postgresql() {
        assert_eq!(DatabaseDialect::PostgreSQL.to_string(), "postgresql");
    }

    #[test]
    fn test_database_dialect_display_generic() {
        assert_eq!(DatabaseDialect::Generic.to_string(), "generic");
    }

    #[test]
    fn test_database_dialect_roundtrip() {
        // Test that Display -> FromStr roundtrip works
        let dialects = vec![
            DatabaseDialect::MySQL,
            DatabaseDialect::SQLite,
            DatabaseDialect::PostgreSQL,
            DatabaseDialect::Generic,
        ];
        
        for dialect in dialects {
            let string = dialect.to_string();
            let parsed: DatabaseDialect = string.parse().unwrap();
            assert_eq!(parsed, dialect);
        }
    }

    // ============================================================================
    // Database Tests
    // ============================================================================

    #[test]
    fn test_database_new() {
        let db = Database::new(DatabaseDialect::PostgreSQL);
        assert_eq!(db.dialect, DatabaseDialect::PostgreSQL);
        assert!(db.schemas.is_empty());
    }

    #[test]
    fn test_database_default() {
        let db = Database::default();
        assert_eq!(db.dialect, DatabaseDialect::Generic);
        assert!(db.schemas.is_empty());
    }

    #[test]
    fn test_database_parse_simple_table() {
        let mut db = Database::new(DatabaseDialect::Generic);
        let result = db.parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)");
        
        assert!(result.is_ok());
        assert_eq!(db.schemas.len(), 1);
        
        let schema = db.schemas.get("").unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert!(schema.tables.contains_key("users"));
    }

    #[test]
    fn test_database_parse_qualified_table() {
        let mut db = Database::new(DatabaseDialect::PostgreSQL);
        let result = db.parse_sql("CREATE TABLE public.users (id INTEGER PRIMARY KEY)");
        
        assert!(result.is_ok());
        assert_eq!(db.schemas.len(), 1);
        
        let schema = db.schemas.get("public").unwrap();
        assert_eq!(schema.name, "public");
        assert!(schema.tables.contains_key("users"));
    }

    #[test]
    fn test_database_parse_multiple_tables() {
        let mut db = Database::new(DatabaseDialect::Generic);
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY);
            CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER);
        "#;
        
        let result = db.parse_sql(sql);
        assert!(result.is_ok());
        
        let schema = db.schemas.get("").unwrap();
        assert_eq!(schema.tables.len(), 2);
        assert!(schema.tables.contains_key("users"));
        assert!(schema.tables.contains_key("posts"));
    }

    #[test]
    fn test_database_parse_create_index() {
        let mut db = Database::new(DatabaseDialect::Generic);
        let sql = r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(255));
            CREATE INDEX idx_email ON users (email);
        "#;
        
        let result = db.parse_sql(sql);
        assert!(result.is_ok());
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        assert_eq!(table.indexes.len(), 1);
        assert_eq!(table.indexes[0].name, "idx_email");
    }

    #[test]
    fn test_database_parse_alter_table() {
        let mut db = Database::new(DatabaseDialect::Generic);
        let sql = r#"
            CREATE TABLE users (id INTEGER, email VARCHAR(255));
            ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id);
        "#;
        
        let result = db.parse_sql(sql);
        assert!(result.is_ok());
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        assert!(table.primary_key.is_some());
        assert_eq!(table.primary_key.as_ref().unwrap().columns, vec!["id"]);
    }

    #[test]
    fn test_database_clone() {
        let mut db = Database::new(DatabaseDialect::PostgreSQL);
        db.parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)").unwrap();
        
        let cloned = db.clone();
        assert_eq!(db, cloned);
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
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE SCHEMA myschema; CREATE TABLE myschema.users (id INTEGER)").ok();
        
        // Note: CREATE SCHEMA is not parsed by sqlparser, so we test with qualified table names
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE TABLE myschema.users (id INTEGER PRIMARY KEY)").unwrap();
        
        let schema = db.schemas.get("myschema").unwrap();
        assert_eq!(schema.name, "myschema");
        assert_eq!(schema.tables.len(), 1);
    }

    #[test]
    fn test_schema_clone() {
        let mut schema = Schema::default();
        schema.name = "test".to_string();
        
        let cloned = schema.clone();
        assert_eq!(schema, cloned);
    }

    // ============================================================================
    // Table Tests
    // ============================================================================

    #[test]
    fn test_table_qualified_name_with_schema() {
        let table = Table {
            name: "users".to_string(),
            schema: Some("public".to_string()),
            columns: vec![],
            primary_key: None,
            foreign_keys: vec![],
            indexes: vec![],
        };
        
        assert_eq!(table.qualified_name(), "public.users");
    }

    #[test]
    fn test_table_qualified_name_without_schema() {
        let table = Table {
            name: "users".to_string(),
            schema: None,
            columns: vec![],
            primary_key: None,
            foreign_keys: vec![],
            indexes: vec![],
        };
        
        assert_eq!(table.qualified_name(), "users");
    }

    #[test]
    fn test_table_qualified_name_with_empty_schema() {
        let table = Table {
            name: "users".to_string(),
            schema: Some("".to_string()),
            columns: vec![],
            primary_key: None,
            foreign_keys: vec![],
            indexes: vec![],
        };
        
        assert_eq!(table.qualified_name(), "users");
    }

    #[test]
    fn test_table_has_primary_key_true() {
        let table = Table {
            name: "users".to_string(),
            schema: None,
            columns: vec![],
            primary_key: Some(PrimaryKey {
                name: None,
                columns: vec!["id".to_string()],
            }),
            foreign_keys: vec![],
            indexes: vec![],
        };
        
        assert!(table.has_primary_key());
    }

    #[test]
    fn test_table_has_primary_key_false() {
        let table = Table {
            name: "users".to_string(),
            schema: None,
            columns: vec![],
            primary_key: None,
            foreign_keys: vec![],
            indexes: vec![],
        };
        
        assert!(!table.has_primary_key());
    }

    #[test]
    fn test_table_from_create_table_simple() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255))").unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        
        assert_eq!(table.name, "users");
        assert_eq!(table.schema, None);
        assert_eq!(table.columns.len(), 2);
        assert!(table.has_primary_key());
    }

    #[test]
    fn test_table_from_create_table_with_schema() {
        let mut db = Database::new(DatabaseDialect::PostgreSQL);
        db.parse_sql("CREATE TABLE public.users (id INTEGER)").unwrap();
        
        let schema = db.schemas.get("public").unwrap();
        let table = schema.tables.get("users").unwrap();
        
        assert_eq!(table.name, "users");
        assert_eq!(table.schema, Some("public".to_string()));
    }

    #[test]
    fn test_table_clone() {
        let table = Table {
            name: "users".to_string(),
            schema: None,
            columns: vec![],
            primary_key: None,
            foreign_keys: vec![],
            indexes: vec![],
        };
        
        let cloned = table.clone();
        assert_eq!(table, cloned);
    }

    // ============================================================================
    // Column Tests
    // ============================================================================

    #[test]
    fn test_column_nullable_by_default() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE TABLE users (name VARCHAR(255))").unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        let column = &table.columns[0];
        
        assert_eq!(column.name, "name");
        assert!(column.nullable);
    }

    #[test]
    fn test_column_not_null_constraint() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE TABLE users (name VARCHAR(255) NOT NULL)").unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        let column = &table.columns[0];
        
        assert_eq!(column.name, "name");
        assert!(!column.nullable);
    }

    #[test]
    fn test_column_primary_key_not_nullable() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)").unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        let column = &table.columns[0];
        
        assert_eq!(column.name, "id");
        assert!(!column.nullable);
    }

    #[test]
    fn test_column_default_value() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE TABLE users (status VARCHAR(50) DEFAULT 'active')").unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        let column = &table.columns[0];
        
        assert_eq!(column.name, "status");
        assert!(column.default.is_some());
        assert!(column.default.as_ref().unwrap().contains("active"));
    }

    #[test]
    fn test_column_data_type() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR(255), created_at TIMESTAMP)").unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        
        assert_eq!(table.columns[0].data_type, "INTEGER");
        assert!(table.columns[1].data_type.contains("VARCHAR"));
        assert_eq!(table.columns[2].data_type, "TIMESTAMP");
    }



    #[test]
    fn test_column_clone() {
        let column = Column {
            name: "test".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: true,
            default: Some("0".to_string()),
        };
        
        let cloned = column.clone();
        assert_eq!(column, cloned);
    }

    // ============================================================================
    // Index Tests
    // ============================================================================

    #[test]
    fn test_index_from_create_index() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql(r#"
            CREATE TABLE users (email VARCHAR(255));
            CREATE INDEX idx_email ON users (email);
        "#).unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        
        assert_eq!(table.indexes.len(), 1);
        assert_eq!(table.indexes[0].name, "idx_email");
        assert_eq!(table.indexes[0].columns, vec!["email"]);
        assert!(!table.indexes[0].unique);
    }

    #[test]
    fn test_index_unique() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql(r#"
            CREATE TABLE users (email VARCHAR(255));
            CREATE UNIQUE INDEX idx_email ON users (email);
        "#).unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        
        assert_eq!(table.indexes.len(), 1);
        assert!(table.indexes[0].unique);
    }

    #[test]
    fn test_index_multi_column() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql(r#"
            CREATE TABLE users (first_name VARCHAR(255), last_name VARCHAR(255));
            CREATE INDEX idx_name ON users (first_name, last_name);
        "#).unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        
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
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)").unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        let pk = table.primary_key.as_ref().unwrap();
        
        assert_eq!(pk.columns.len(), 1);
        assert_eq!(pk.columns[0], "id");
    }

    #[test]
    fn test_primary_key_composite() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE TABLE user_roles (user_id INTEGER, role_id INTEGER, PRIMARY KEY (user_id, role_id))").unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("user_roles").unwrap();
        let pk = table.primary_key.as_ref().unwrap();
        
        assert_eq!(pk.columns.len(), 2);
        assert_eq!(pk.columns, vec!["user_id", "role_id"]);
    }

    #[test]
    fn test_primary_key_named_constraint() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE TABLE users (id INTEGER, CONSTRAINT pk_users PRIMARY KEY (id))").unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("users").unwrap();
        let pk = table.primary_key.as_ref().unwrap();
        
        assert!(pk.name.is_some());
        assert_eq!(pk.name.as_ref().unwrap(), "pk_users");
    }

    #[test]
    fn test_primary_key_contains() {
        let pk = PrimaryKey {
            name: None,
            columns: vec!["id".to_string(), "tenant_id".to_string()],
        };
        
        assert!(pk.contains("id"));
        assert!(pk.contains("tenant_id"));
        assert!(!pk.contains("email"));
    }

    #[test]
    fn test_primary_key_clone() {
        let pk = PrimaryKey {
            name: Some("pk_users".to_string()),
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
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql("CREATE TABLE posts (id INTEGER, user_id INTEGER REFERENCES users(id))").unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("posts").unwrap();
        
        assert_eq!(table.foreign_keys.len(), 1);
        let fk = &table.foreign_keys[0];
        assert_eq!(fk.columns, vec!["user_id"]);
        assert_eq!(fk.referenced_table, "users");
        assert_eq!(fk.referenced_columns, vec!["id"]);
    }

    #[test]
    fn test_foreign_key_table_constraint() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql(r#"
            CREATE TABLE posts (
                id INTEGER,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        "#).unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("posts").unwrap();
        
        assert_eq!(table.foreign_keys.len(), 1);
        let fk = &table.foreign_keys[0];
        assert_eq!(fk.columns, vec!["user_id"]);
        assert_eq!(fk.referenced_table, "users");
    }

    #[test]
    fn test_foreign_key_named_constraint() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql(r#"
            CREATE TABLE posts (
                id INTEGER,
                user_id INTEGER,
                CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)
            )
        "#).unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("posts").unwrap();
        let fk = &table.foreign_keys[0];
        
        assert!(fk.name.is_some());
        assert_eq!(fk.name.as_ref().unwrap(), "fk_user");
    }

    #[test]
    fn test_foreign_key_on_delete() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql(r#"
            CREATE TABLE posts (
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE
            )
        "#).unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("posts").unwrap();
        let fk = &table.foreign_keys[0];
        
        assert!(fk.on_delete.is_some());
        assert!(fk.on_delete.as_ref().unwrap().contains("CASCADE"));
    }

    #[test]
    fn test_foreign_key_on_update() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql(r#"
            CREATE TABLE posts (
                user_id INTEGER REFERENCES users(id) ON UPDATE CASCADE
            )
        "#).unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("posts").unwrap();
        let fk = &table.foreign_keys[0];
        
        assert!(fk.on_update.is_some());
        assert!(fk.on_update.as_ref().unwrap().contains("CASCADE"));
    }

    #[test]
    fn test_foreign_key_composite() {
        let mut db = Database::new(DatabaseDialect::Generic);
        db.parse_sql(r#"
            CREATE TABLE order_items (
                order_id INTEGER,
                product_id INTEGER,
                FOREIGN KEY (order_id, product_id) REFERENCES orders(id, product_id)
            )
        "#).unwrap();
        
        let schema = db.schemas.get("").unwrap();
        let table = schema.tables.get("order_items").unwrap();
        let fk = &table.foreign_keys[0];
        
        assert_eq!(fk.columns.len(), 2);
        assert_eq!(fk.columns, vec!["order_id", "product_id"]);
        assert_eq!(fk.referenced_columns.len(), 2);
    }

    #[test]
    fn test_foreign_key_references() {
        let fk = ForeignKey {
            name: None,
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        };
        
        assert!(fk.references("users"));
        assert!(!fk.references("posts"));
    }

    #[test]
    fn test_foreign_key_contains() {
        let fk = ForeignKey {
            name: None,
            columns: vec!["user_id".to_string(), "tenant_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string(), "tenant_id".to_string()],
            on_delete: None,
            on_update: None,
        };
        
        assert!(fk.contains("user_id"));
        assert!(fk.contains("tenant_id"));
        assert!(!fk.contains("post_id"));
    }

    #[test]
    fn test_foreign_key_clone() {
        let fk = ForeignKey {
            name: Some("fk_user".to_string()),
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: Some("CASCADE".to_string()),
            on_update: None,
        };
        
        let cloned = fk.clone();
        assert_eq!(fk, cloned);
    }

    // ============================================================================
    // Integration Tests
    // ============================================================================

    #[test]
    fn test_complete_schema_parsing() {
        let mut db = Database::new(DatabaseDialect::PostgreSQL);
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
        
        db.parse_sql(sql).unwrap();
        
        let schema = db.schemas.get("").unwrap();
        assert_eq!(schema.tables.len(), 2);
        
        let users_table = schema.tables.get("users").unwrap();
        assert_eq!(users_table.columns.len(), 3);
        assert!(users_table.has_primary_key());
        assert_eq!(users_table.indexes.len(), 1);
        
        let posts_table = schema.tables.get("posts").unwrap();
        assert_eq!(posts_table.foreign_keys.len(), 1);
        assert_eq!(posts_table.indexes.len(), 1);
    }

    #[test]
    fn test_multiple_schemas() {
        let mut db = Database::new(DatabaseDialect::PostgreSQL);
        let sql = r#"
            CREATE TABLE public.users (id INTEGER PRIMARY KEY);
            CREATE TABLE auth.sessions (id INTEGER PRIMARY KEY);
        "#;
        
        db.parse_sql(sql).unwrap();
        
        assert_eq!(db.schemas.len(), 2);
        assert!(db.schemas.contains_key("public"));
        assert!(db.schemas.contains_key("auth"));
    }
}
