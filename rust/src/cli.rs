/*
 * Copyright (C) 2023-2026  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Utilities to build a Command-Line Interface

use std::path::Path;

use anyhow::{Context, Result};
use clap::ValueEnum;

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum MphAlgorithm {
    Fmphgo,
    Pthash,
    Cmph,
}

pub fn load_mph(mph_algo: MphAlgorithm, path: &Path) -> Result<crate::mph::DynMphf> {
    Ok(match mph_algo {
        MphAlgorithm::Cmph => crate::java_compat::mph::gov::GOVMPH::load(path)
            .context("Cannot load mph")?
            .into(),
        MphAlgorithm::Fmphgo => crate::mph::SwhidFmphgo::load(path)
            .context("Cannot load mph")?
            .into(),
        MphAlgorithm::Pthash => {
            #[cfg(not(feature = "pthash"))]
            anyhow::bail!("pthash support is disabled. Recompile with --features pthash");
            #[cfg(feature = "pthash")]
            crate::mph::SwhidPthash::load(path)
                .context("Cannot load mph")?
                .into()
        }
    })
}
