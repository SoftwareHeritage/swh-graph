// Examples use PTHash which uses the FFI, and Miri does not support that.
#![cfg_attr(not(miri), doc = include_str!("../tutorial.md"))]
