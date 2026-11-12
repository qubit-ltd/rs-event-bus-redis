// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fs;

const START_MARKER: &str = "<!-- acl-example: event-worker -->";
const END_MARKER: &str = "<!-- end-acl-example -->";

fn marker_is_outside_fenced_block(contents: &str, marker: &str) -> bool {
    let mut fence: Option<(u8, usize)> = None;
    for line in contents.lines() {
        if line == marker {
            return fence.is_none();
        }
        let indent = line.bytes().take_while(|byte| *byte == b' ').count();
        if indent > 3 {
            continue;
        }
        let candidate = &line[indent..];
        if let Some((delimiter, minimum_length)) = fence {
            let length = candidate.bytes().take_while(|byte| *byte == delimiter).count();
            if length >= minimum_length && candidate[length..].trim().is_empty() {
                fence = None;
            }
        } else if let Some(delimiter @ (b'`' | b'~')) = candidate.as_bytes().first().copied() {
            let length = candidate.bytes().take_while(|byte| *byte == delimiter).count();
            if length >= 3 && (delimiter != b'`' || !candidate[length..].contains('`')) {
                fence = Some((delimiter, length));
            }
        }
    }
    false
}

fn copied_acl_command(contents: &str) -> Result<&str, &'static str> {
    if contents.matches(START_MARKER).count() != 1 || contents.matches(END_MARKER).count() != 1 {
        return Err("expected unique ACL markers");
    }
    if contents.lines().filter(|line| *line == START_MARKER).count() != 1
        || contents.lines().filter(|line| *line == END_MARKER).count() != 1
    {
        return Err("expected ACL markers on separate lines");
    }
    if !marker_is_outside_fenced_block(contents, START_MARKER) || !marker_is_outside_fenced_block(contents, END_MARKER)
    {
        return Err("expected ACL markers outside fenced blocks");
    }
    let (before_end, _) = contents.split_once(END_MARKER).ok_or("missing end marker")?;
    let (_, between) = before_end.split_once(START_MARKER).ok_or("invalid marker order")?;
    let block = between.trim();
    let mut lines = block.lines();
    if lines.next() != Some("```text") {
        return Err("expected one text fenced block");
    }
    let command = lines.next().ok_or("expected one copyable ACL command")?;
    if lines.next() != Some("```") || lines.next().is_some() {
        return Err("expected one text fenced block");
    }
    if command.is_empty()
        || command.trim() != command
        || !command.starts_with("ACL SETUSER ")
        || command.contains('\\')
        || command.contains("<!--")
        || command.contains("```")
    {
        return Err("expected one copyable ACL command");
    }
    Ok(command)
}

pub(crate) fn documented_arguments(path: &str) -> Vec<String> {
    let contents = fs::read_to_string(path).unwrap();
    let command = copied_acl_command(&contents).unwrap_or_else(|error| panic!("{path}: {error}"));
    command.split_ascii_whitespace().map(str::to_owned).collect()
}

pub(crate) const GUIDES: [&str; 2] = ["doc/user_guide.md", "doc/user_guide.zh_CN.md"];

#[cfg(test)]
mod tests {
    use super::END_MARKER;
    use super::START_MARKER;
    use super::copied_acl_command;

    const COMMAND: &str = "ACL SETUSER example reset on";

    fn document(block: &str) -> String {
        format!("<!-- acl-example: event-worker -->\n{block}\n<!-- end-acl-example -->")
    }

    #[test]
    fn test_acl_copy_block_accepts_complete_text_fence() {
        let valid = document(&format!("```text\n{COMMAND}\n```"));
        assert_eq!(copied_acl_command(&valid), Ok(COMMAND));
    }

    #[test]
    fn test_acl_copy_block_rejects_old_layout() {
        let old = format!("```text\n<!-- acl-example: event-worker -->\n{COMMAND}\n<!-- end-acl-example -->\n```");
        assert!(copied_acl_command(&old).is_err());
    }

    #[test]
    fn test_acl_copy_block_rejects_markers_inside_an_outer_fence() {
        let nested = format!("```text\n{START_MARKER}\n```text\n{COMMAND}\n```\n{END_MARKER}\n```");
        assert!(copied_acl_command(&nested).is_err());
    }

    #[test]
    fn test_acl_copy_block_rejects_mixed_or_incomplete_contents() {
        for block in [
            format!("```text\n{COMMAND}\n<!-- extra -->\n```"),
            format!("```text\n{COMMAND}\nACL SETUSER other on\n```"),
            format!("```text\n{COMMAND}\\\nnext\n```"),
            format!("```text\n{COMMAND}"),
            format!("```text\n{COMMAND}\n```\n```text\n{COMMAND}\n```"),
            format!("```\n{COMMAND}\n```"),
        ] {
            assert!(copied_acl_command(&document(&block)).is_err(), "accepted {block:?}");
        }
    }

    #[test]
    fn test_acl_copy_block_rejects_duplicate_or_reordered_markers() {
        let duplicate = format!(
            "{0}\n```text\n{COMMAND}\n```\n{0}\n{1}",
            "<!-- acl-example: event-worker -->", "<!-- end-acl-example -->"
        );
        assert!(copied_acl_command(&duplicate).is_err());
        let reversed = format!("<!-- end-acl-example -->\n<!-- acl-example: event-worker -->\n```text\n{COMMAND}\n```");
        assert!(copied_acl_command(&reversed).is_err());
    }

    #[test]
    fn test_acl_copy_block_accepts_crlf_documents() {
        let valid =
            format!("<!-- acl-example: event-worker -->\r\n```text\r\n{COMMAND}\r\n```\r\n<!-- end-acl-example -->");
        assert_eq!(copied_acl_command(&valid), Ok(COMMAND));
    }
}
