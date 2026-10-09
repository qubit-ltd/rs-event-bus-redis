use super::redis_error_code_class;

#[test]
fn test_server_error_codes_map_to_stable_categories() {
    for (code, expected) in [
        ("NOAUTH", Some(("authentication", Some(false)))),
        ("WRONGPASS", Some(("authentication", Some(false)))),
        ("NOPERM", Some(("authentication", Some(false)))),
        ("WRONGTYPE", Some(("wrong_type", Some(false)))),
        ("OOM", Some(("out_of_memory", Some(false)))),
        ("LOADING", Some(("temporarily_unavailable", Some(true)))),
        ("TRYAGAIN", Some(("temporarily_unavailable", Some(true)))),
        ("MASTERDOWN", Some(("temporarily_unavailable", Some(true)))),
        ("READONLY", Some(("temporarily_unavailable", Some(true)))),
        ("MOVED", Some(("unsupported_topology", Some(false)))),
        ("ASK", Some(("unsupported_topology", Some(false)))),
        ("CROSSSLOT", Some(("unsupported_topology", Some(false)))),
        ("CLUSTERDOWN", Some(("unsupported_topology", Some(false)))),
        ("ERR", None),
    ] {
        assert_eq!(
            redis_error_code_class(code),
            expected,
            "unexpected mapping for {code}"
        );
    }
}
