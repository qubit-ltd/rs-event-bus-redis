// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fs;

pub(crate) fn documented_arguments(path: &str) -> Vec<String> {
    let contents = fs::read_to_string(path).unwrap();
    let start_marker = "<!-- acl-example: event-worker -->";
    let end_marker = "<!-- end-acl-example -->";
    assert_eq!(contents.matches(start_marker).count(), 1, "{path}");
    assert_eq!(contents.matches(end_marker).count(), 1, "{path}");
    let example = contents
        .split_once(start_marker)
        .unwrap()
        .1
        .split_once(end_marker)
        .unwrap()
        .0;
    let lines: Vec<_> = example.lines().map(str::trim).filter(|line| !line.is_empty()).collect();
    assert_eq!(lines.len(), 1, "{path}: expected one Redis console command");
    assert!(
        !lines[0].contains('\\'),
        "{path}: command must not use line continuation"
    );
    lines[0].split_ascii_whitespace().map(str::to_owned).collect()
}

pub(crate) const GUIDES: [&str; 2] = ["doc/user_guide.md", "doc/user_guide.zh_CN.md"];
