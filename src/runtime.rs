use crate::plugin::{GenerateRequest, GenerateResponse};
use prost::Message;
use std::error::Error;
use std::io::{Read, Write};

pub fn run<TFunc>(process: TFunc) -> Result<(), Box<dyn Error>>
where
    TFunc: FnOnce(GenerateRequest) -> Result<GenerateResponse, Box<dyn Error>>,
{
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    run_with_io(stdin.lock(), stdout.lock(), process)
}

pub fn run_with_io<TReader, TWriter, TFunc>(
    mut reader: TReader,
    mut writer: TWriter,
    process: TFunc,
) -> Result<(), Box<dyn Error>>
where
    TReader: Read,
    TWriter: Write,
    TFunc: FnOnce(GenerateRequest) -> Result<GenerateResponse, Box<dyn Error>>,
{
    let mut input = Vec::new();
    reader.read_to_end(&mut input)?;

    let request = GenerateRequest::decode(&input[..])?;
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
