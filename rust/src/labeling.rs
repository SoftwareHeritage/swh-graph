// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use bitstream::Supply;
use dsi_bitstream::codes::GammaRead;
use dsi_bitstream::impls::{BufBitReader, MemWordReader};
use dsi_bitstream::traits::{BitRead, BitSeek, Endianness, BE};
use epserde::deser::{DeserType, Deserialize, Flags, MemCase};
use mmap_rs::MmapFlags;
use std::path::Path;
use webgraph::prelude::bitstream::BitStreamLabeling;
use webgraph::prelude::*;

/// A [`BitDeserializer`] for the labels stored in the bitstream.
///
/// Labels are deserialized as a sequence of `u64` values, each of which is
/// `width` bits wide. The length of the sequence is read using a [γ
/// code](GammaRead), and then each value is obtained by reading `width` bits.
pub struct SwhDeserializer {
    width: usize,
}

impl SwhDeserializer {
    /// Creates a new [`SwhDeserializer`] with the given width.
    pub(crate) fn new(width: usize) -> Self {
        Self { width }
    }
}

impl<BR: BitRead<BE> + BitSeek + GammaRead<BE>> BitDeserializer<BE, BR> for SwhDeserializer {
    type DeserType = Vec<u64>;

    fn deserialize(
        &self,
        bitstream: &mut BR,
    ) -> std::result::Result<Self::DeserType, <BR as BitRead<BE>>::Error> {
        let num_labels = bitstream.read_gamma().unwrap() as usize;
        let mut labels = Vec::with_capacity(num_labels);
        for _ in 0..num_labels {
            labels.push(bitstream.read_bits(self.width)?);
        }
        Ok(labels)
    }
}

pub struct MmapReaderSupplier<E: Endianness> {
    backend: MmapHelper<u32>,
    _marker: std::marker::PhantomData<E>,
}

impl Supply for MmapReaderSupplier<BE> {
    type Item<'a>
        = BufBitReader<BE, MemWordReader<u32, &'a [u32]>>
    where
        Self: 'a;

    fn request(&self) -> Self::Item<'_> {
        BufBitReader::<BE, _>::new(MemWordReader::new(self.backend.as_ref()))
    }
}

pub type SwhLabelingInner =
    BitStreamLabeling<BE, MmapReaderSupplier<BE>, SwhDeserializer, MemCase<DeserType<'static, EF>>>;

pub struct SwhLabeling(pub SwhLabelingInner);

impl SequentialLabeling for SwhLabeling {
    type Label = <SwhLabelingInner as SequentialLabeling>::Label;
    type Lender<'node>
        = <SwhLabelingInner as SequentialLabeling>::Lender<'node>
    where
        Self: 'node;

    // Required methods
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        self.0.iter_from(from)
    }
}

impl RandomAccessLabeling for SwhLabeling {
    type Labels<'succ>
        = <SwhLabelingInner as RandomAccessLabeling>::Labels<'succ>
    where
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        <SwhLabelingInner as RandomAccessLabeling>::num_arcs(&self.0)
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        <SwhLabelingInner as RandomAccessLabeling>::labels(&self.0, node_id)
    }

    fn outdegree(&self, node_id: usize) -> usize {
        <SwhLabelingInner as RandomAccessLabeling>::outdegree(&self.0, node_id)
    }
}

pub(crate) fn mmap(path: impl AsRef<Path>, bit_deser: SwhDeserializer) -> Result<SwhLabeling> {
    let path = path.as_ref();
    let labels_path = path.with_extension("labels");
    let ef_path = path.with_extension("ef");
    let ef = EF::mmap(&ef_path, Flags::empty())
        .with_context(|| format!("Could not parse {}", ef_path.display()))?;
    Ok(SwhLabeling(BitStreamLabeling::new(
        MmapReaderSupplier {
            backend: MmapHelper::<u32>::mmap(&labels_path, MmapFlags::empty())
                .with_context(|| format!("Could not mmap {}", labels_path.display()))?,
            _marker: std::marker::PhantomData,
        },
        bit_deser,
        ef,
    )))
}
