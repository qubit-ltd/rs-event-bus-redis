// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RESP2 request and response framing helpers for the local proxy.

use std::io::BufReader;
use std::io::Read;
use std::net::TcpStream;

pub(super) fn read_request(reader: &mut BufReader<TcpStream>) -> std::io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let Some((line, mut wire)) = read_line(reader)? else {
        return Ok(None);
    };
    if line.first() != Some(&b'*') {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected RESP array",
        ));
    }
    let count = parse_length(&line[1..])?;
    let mut command = Vec::new();
    for index in 0..count {
        let (header, mut header_wire) = read_line(reader)?.ok_or_else(unexpected_eof)?;
        if header.first() != Some(&b'$') {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected RESP bulk string",
            ));
        }
        let length = usize::try_from(parse_length(&header[1..])?)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid RESP bulk length"))?;
        let mut value = vec![0; length];
        reader.read_exact(&mut value)?;
        let mut ending = [0; 2];
        reader.read_exact(&mut ending)?;
        header_wire.extend_from_slice(&value);
        header_wire.extend_from_slice(&ending);
        wire.extend_from_slice(&header_wire);
        if index == 0 {
            command = value;
        }
    }
    Ok(Some((command, wire)))
}

pub(super) fn read_response(reader: &mut BufReader<TcpStream>) -> std::io::Result<Vec<u8>> {
    let (line, mut wire) = read_line(reader)?.ok_or_else(unexpected_eof)?;
    match line.first() {
        Some(b'$') => {
            let length = parse_length(&line[1..])?;
            if length >= 0 {
                let mut body = vec![0; length as usize + 2];
                reader.read_exact(&mut body)?;
                wire.extend_from_slice(&body);
            }
        }
        Some(b'*') => {
            let count = parse_length(&line[1..])?;
            if count >= 0 {
                for _ in 0..count {
                    wire.extend_from_slice(&read_response(reader)?);
                }
            }
        }
        _ => {}
    }
    Ok(wire)
}

fn read_line(reader: &mut BufReader<TcpStream>) -> std::io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut wire = Vec::new();
    let mut byte = [0];
    loop {
        match reader.read_exact(&mut byte) {
            Ok(()) => wire.push(byte[0]),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof && wire.is_empty() => return Ok(None),
            Err(error) => return Err(error),
        }
        if wire.ends_with(b"\r\n") {
            return Ok(Some((wire[..wire.len() - 2].to_vec(), wire)));
        }
    }
}

fn parse_length(bytes: &[u8]) -> std::io::Result<isize> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid RESP length"))
}

fn unexpected_eof() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated RESP response")
}
