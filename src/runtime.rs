//! Runtime infrastructure for sqlc plugin execution.
//!
//! This module provides the core runtime functions for sqlc plugins, handling
//! the communication protocol between sqlc and the plugin. It reads protobuf-encoded
//! requests from stdin, processes them through a user-provided handler, and writes
//! protobuf-encoded responses to stdout.
//!
//! # Overview
//!
//! The runtime handles:
//! - Reading and decoding protobuf messages from stdin
//! - Invoking user-defined code generation logic
//! - Encoding and writing responses back to stdout
//! - Error propagation and handling
//!
//! # Example
//!
//! ```no_run
//! use sqlc_gen_core::plugin::{GenerateRequest, GenerateResponse, File};
//! use sqlc_gen_core::runtime::run;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     run(|request| {
//!         // Your code generation logic here
//!         let files = vec![File {
//!             name: "output.rs".to_string(),
//!             contents: b"// Generated code".to_vec(),
//!         }];
//!         
//!         Ok(GenerateResponse { files })
//!     })
//! }
//! ```

use crate::plugin::{GenerateRequest, GenerateResponse};
use crate::schema::CatalogBuilder;
use prost::Message;
use std::error::Error;
use std::io::{Read, Write};

/// Runs a sqlc plugin with the standard stdin/stdout communication protocol.
///
/// This is the main entry point for sqlc plugins. It reads a protobuf-encoded
/// [`GenerateRequest`] from stdin, passes it to your processing function, and
/// writes the resulting [`GenerateResponse`] back to stdout.
///
/// # Arguments
///
/// * `process` - A function that takes a [`GenerateRequest`] and returns a
///   [`GenerateResponse`]. This is where your code generation logic should live.
///
/// # Errors
///
/// Returns an error if:
/// - Reading from stdin fails
/// - Decoding the protobuf request fails
/// - The process function returns an error
/// - Encoding the response fails
/// - Writing to stdout fails
///
/// # Example
///
/// ```no_run
/// use sqlc_gen_core::plugin::{GenerateRequest, GenerateResponse, File};
/// use sqlc_gen_core::runtime::run;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     run(|request| {
///         // Access request data
///         let version = &request.sqlc_version;
///         let queries = &request.queries;
///         
///         // Generate files based on the request
///         let files = vec![File {
///             name: "generated.rs".to_string(),
///             contents: b"// Generated code".to_vec(),
///         }];
///         
///         Ok(GenerateResponse { files })
///     })
/// }
/// ```
pub fn run<F>(process: F) -> Result<(), Box<dyn Error>>
where
    F: FnOnce(GenerateRequest) -> Result<GenerateResponse, Box<dyn Error>>,
{
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    run_with_io(stdin.lock(), stdout.lock(), process)
}

/// Runs a sqlc plugin with custom I/O streams.
///
/// This is a more flexible version of [`run`] that allows you to provide custom
/// readers and writers instead of using stdin/stdout. This is particularly useful
/// for testing, as it allows you to pass in-memory buffers instead of actual I/O streams.
///
/// # Arguments
///
/// * `reader` - An input stream containing the protobuf-encoded [`GenerateRequest`]
/// * `writer` - An output stream where the protobuf-encoded [`GenerateResponse`] will be written
/// * `process` - A function that takes a [`GenerateRequest`] and returns a [`GenerateResponse`]
///
/// # Type Parameters
///
/// * `R` - Any type that implements [`Read`]
/// * `W` - Any type that implements [`Write`]
/// * `F` - A closure that processes the request and returns a response
///
/// # Errors
///
/// Returns an error if:
/// - Reading from the input stream fails
/// - Decoding the protobuf request fails
/// - The process function returns an error
/// - Encoding the response fails
/// - Writing to the output stream fails
///
/// # Example
///
/// ```
/// use sqlc_gen_core::plugin::{GenerateRequest, GenerateResponse, File};
/// use sqlc_gen_core::runtime::run_with_io;
/// use prost::Message;
///
/// // Create a test request
/// let request = GenerateRequest {
///     sqlc_version: "1.0.0".to_string(),
///     settings: None,
///     catalog: None,
///     queries: vec![],
///     plugin_options: vec![],
///     global_options: vec![],
/// };
///
/// // Encode it
/// let mut input = Vec::new();
/// request.encode(&mut input).unwrap();
///
/// // Process it
/// let mut output = Vec::new();
/// run_with_io(&input[..], &mut output, |req| {
///     assert_eq!(req.sqlc_version, "1.0.0");
///     Ok(GenerateResponse { files: vec![] })
/// }).unwrap();
///
/// // Decode the response
/// let response = GenerateResponse::decode(&output[..]).unwrap();
/// assert_eq!(response.files.len(), 0);
/// ```
pub fn run_with_io<R, W, F>(mut reader: R, mut writer: W, process: F) -> Result<(), Box<dyn Error>>
where
    R: Read,
    W: Write,
    F: FnOnce(GenerateRequest) -> Result<GenerateResponse, Box<dyn Error>>,
{
    let mut input = Vec::new();
    reader.read_to_end(&mut input)?;

    let mut request = GenerateRequest::decode(&input[..])?;

    if let Some(settings) = &request.settings {
        if !settings.schema.is_empty() {
            let mut builder = CatalogBuilder::new(settings.engine.as_str());

            for item in &settings.schema {
                let schema = std::fs::read_to_string(item)?;
                builder.parse_sql(&schema)?;
            }

            if let Some(catalog) = request.catalog.take() {
                builder.merge_catalog(catalog);
            }

            request.catalog = Some(builder.build());
        }
    }

    let response = process(request)?;
    let mut output = Vec::new();
    response.encode(&mut output)?;

    writer.write_all(&output)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::{File, GenerateRequest, GenerateResponse};

    fn create_sample_request() -> GenerateRequest {
        GenerateRequest {
            settings: None,
            catalog: None,
            queries: vec![],
            sqlc_version: "test".to_string(),
            plugin_options: vec![],
            global_options: vec![],
        }
    }

    fn create_sample_response() -> GenerateResponse {
        GenerateResponse {
            files: vec![File {
                name: "test.rs".to_string(),
                contents: b"// test content".to_vec(),
            }],
        }
    }

    #[test]
    fn test_run_with_io_success() {
        let mut input = Vec::new();
        let mut output = Vec::new();

        let request = create_sample_request();
        request.encode(&mut input).unwrap();

        let result = run_with_io(&input[..], &mut output, |req| {
            assert_eq!(req.sqlc_version, "test");
            Ok(create_sample_response())
        });
        assert!(result.is_ok(), "run_with_io should succeed");

        let response = GenerateResponse::decode(&output[..]).unwrap();
        assert_eq!(response.files.len(), 1);
        assert_eq!(response.files[0].name, "test.rs");
        assert_eq!(response.files[0].contents, b"// test content");
    }

    #[test]
    fn test_run_with_io_processor_error() {
        let mut input = Vec::new();
        let mut output = Vec::new();

        let request = create_sample_request();
        request.encode(&mut input).unwrap();

        let result = run_with_io(&input[..], &mut output, |_req| {
            Err("Processing failed".into())
        });
        assert!(
            result.is_err(),
            "run_with_io should fail when processor fails"
        );
        assert_eq!(result.unwrap_err().to_string(), "Processing failed");
    }

    #[test]
    fn test_run_with_io_invalid_input() {
        let input = b"invalid protobuf data";
        let mut output = Vec::new();

        let result = run_with_io(&input[..], &mut output, |_req| Ok(create_sample_response()));
        assert!(
            result.is_err(),
            "run_with_io should fail with invalid input"
        );
    }

    #[test]
    fn test_run_with_io_empty_input() {
        let input: &[u8] = &[];
        let mut output = Vec::new();

        let result = run_with_io(input, &mut output, |_req| Ok(create_sample_response()));
        assert!(
            result.is_ok(),
            "run_with_io should succeed with empty input (creates default request)"
        );

        let response = GenerateResponse::decode(&output[..]).unwrap();
        assert_eq!(response.files.len(), 1);
    }

    #[test]
    fn test_run_with_io_empty_response() {
        let mut input = Vec::new();
        let mut output = Vec::new();

        let request = create_sample_request();
        request.encode(&mut input).unwrap();

        // Processor returns empty response
        let result = run_with_io(&input[..], &mut output, |_req| {
            Ok(GenerateResponse { files: vec![] })
        });
        assert!(
            result.is_ok(),
            "run_with_io should succeed with empty response"
        );

        let response = GenerateResponse::decode(&output[..]).unwrap();
        assert_eq!(response.files.len(), 0);
    }

    #[test]
    fn test_run_with_io_multiple_files() {
        let mut input = Vec::new();
        let mut output = Vec::new();

        let request = create_sample_request();
        request.encode(&mut input).unwrap();

        let result = run_with_io(&input[..], &mut output, |_req| {
            Ok(GenerateResponse {
                files: vec![
                    File {
                        name: "file1.rs".to_string(),
                        contents: b"content1".to_vec(),
                    },
                    File {
                        name: "file2.rs".to_string(),
                        contents: b"content2".to_vec(),
                    },
                ],
            })
        });
        assert!(result.is_ok(), "run_with_io should succeed");

        let response = GenerateResponse::decode(&output[..]).unwrap();
        assert_eq!(response.files.len(), 2);
        assert_eq!(response.files[0].name, "file1.rs");
        assert_eq!(response.files[1].name, "file2.rs");
    }

    #[test]
    fn test_run_with_io_preserves_request_data() {
        let mut input = Vec::new();
        let mut output = Vec::new();

        let request = GenerateRequest {
            settings: None,
            catalog: None,
            queries: vec![],
            sqlc_version: "1.2.3".to_string(),
            plugin_options: b"test_plugin_options".to_vec(),
            global_options: b"test_global_options".to_vec(),
        };
        request.encode(&mut input).unwrap();

        let result = run_with_io(&input[..], &mut output, |req| {
            assert_eq!(req.sqlc_version, "1.2.3");
            assert_eq!(req.plugin_options, b"test_plugin_options");
            assert_eq!(req.global_options, b"test_global_options");
            Ok(create_sample_response())
        });
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_with_io_large_content() {
        let mut input = Vec::new();
        let mut output = Vec::new();

        let request = create_sample_request();
        request.encode(&mut input).unwrap();

        let result = run_with_io(&input[..], &mut output, |_req| {
            Ok(GenerateResponse {
                files: vec![File {
                    name: "large.rs".to_string(),
                    contents: vec![b'x'; 1024 * 1024].clone(),
                }],
            })
        });
        assert!(result.is_ok(), "run_with_io should handle large content");

        let response = GenerateResponse::decode(&output[..]).unwrap();
        assert_eq!(response.files[0].contents.len(), 1024 * 1024);
    }
}
