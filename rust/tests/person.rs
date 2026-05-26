// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use swh_graph::person::*;

#[test]
fn parse_valid_fullname() {
    let fullname = parse_fullname("Person Full Name <account@domain.name>").unwrap();
    assert_eq!(
        fullname,
        ParsedFullname {
            name: "Person Full Name",
            email: "account@domain.name",
            email_localpart: "account",
            email_domain: "domain.name",
        }
    );

    assert_eq!(
        parse_fullname("Some One <user@localhost>")
            .unwrap()
            .email_domain,
        "localhost"
    );
}

#[test]
fn parse_invalid_fullname() {
    assert!(
        parse_fullname("Person Full Name <account@domain.name").is_err(),
        "Allowed fullname without closing bracket"
    );
    assert!(
        parse_fullname("Person Full Name").is_err(),
        "Allowed fullname without email"
    );
    assert!(
        parse_fullname(" <account@domain.name>").is_err(),
        "Allowed fullname without name"
    );
    assert!(parse_fullname("").is_err(), "Allowed empty fullname");
}
