/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.rpc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.softwareheritage.graph.SWHID;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class GetNodeTest extends TraversalServiceTest {
    @Test
    public void testNotFound() {
        StatusRuntimeException thrown = assertThrows(StatusRuntimeException.class,
                () -> client.getNode(GetNodeRequest.newBuilder().setSwhid(fakeSWHID("cnt", 404).toString()).build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void testInvalidSwhid() {
        StatusRuntimeException thrown;
        thrown = assertThrows(StatusRuntimeException.class, () -> client.getNode(
                GetNodeRequest.newBuilder().setSwhid("swh:1:lol:0000000000000000000000000000000000000001").build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
        thrown = assertThrows(StatusRuntimeException.class, () -> client.getNode(
                GetNodeRequest.newBuilder().setSwhid("swh:1:cnt:000000000000000000000000000000000000000z").build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void testContents() {
        List<Integer> expectedCnts = List.of(1, 4, 5, 7, 11, 14, 15);
        Map<Integer, Integer> expectedLengths = Map.of(1, 42, 4, 404, 5, 1337, 7, 666, 11, 313, 14, 14, 15, 404);
        Set<Integer> expectedSkipped = Set.of(15);

        for (Integer cntId : expectedCnts) {
            Node n = client.getNode(GetNodeRequest.newBuilder().setSwhid(fakeSWHID("cnt", cntId).toString()).build());
            assertTrue(n.hasCnt());
            assertTrue(n.getCnt().hasLength());
            assertEquals((long) expectedLengths.get(cntId), n.getCnt().getLength());
            assertTrue(n.getCnt().hasIsSkipped());
            assertEquals(expectedSkipped.contains(cntId), n.getCnt().getIsSkipped());
        }
    }

    @Test
    public void testRevisions() {
        List<Integer> expectedRevs = List.of(3, 9, 13, 18);
        Map<Integer, String> expectedMessages = Map.of(3, "Initial commit", 9, "Add parser", 13, "Add tests", 18,
                "Refactor codebase");

        Map<Integer, String> expectedAuthors = Map.of(3, "foo", 9, "bar", 13, "foo", 18, "baz");
        Map<Integer, String> expectedCommitters = Map.of(3, "foo", 9, "bar", 13, "bar", 18, "foo");

        Map<Integer, Long> expectedAuthorTimestamps = Map.of(3, 1111122220L, 9, 1111144440L, 13, 1111166660L, 18,
                1111177770L);
        Map<Integer, Long> expectedCommitterTimestamps = Map.of(3, 1111122220L, 9, 1111155550L, 13, 1111166660L, 18,
                1111177770L);
        Map<Integer, Integer> expectedAuthorTimestampOffsets = Map.of(3, 120, 9, 120, 13, 120, 18, 0);
        Map<Integer, Integer> expectedCommitterTimestampOffsets = Map.of(3, 120, 9, 120, 13, 120, 18, 0);

        HashMap<Integer, String> personMapping = new HashMap<>();
        for (Integer revId : expectedRevs) {
            Node n = client.getNode(GetNodeRequest.newBuilder().setSwhid(fakeSWHID("rev", revId).toString()).build());
            assertTrue(n.hasRev());
            assertTrue(n.getRev().hasMessage());
            assertEquals(expectedMessages.get(revId), n.getRev().getMessage().toStringUtf8());

            // Persons are anonymized, we just need to check that the mapping is self-consistent
            assertTrue(n.getRev().hasAuthor());
            assertTrue(n.getRev().hasCommitter());
            int[] actualPersons = new int[]{(int) n.getRev().getAuthor(), (int) n.getRev().getCommitter()};
            String[] expectedPersons = new String[]{expectedAuthors.get(revId), expectedCommitters.get(revId)};
            for (int i = 0; i < actualPersons.length; i++) {
                int actualPerson = actualPersons[i];
                String expectedPerson = expectedPersons[i];
                assertTrue(actualPerson >= 0);
                if (personMapping.containsKey(actualPerson)) {
                    assertEquals(personMapping.get(actualPerson), expectedPerson);
                } else {
                    personMapping.put(actualPerson, expectedPerson);
                }
            }

            assertTrue(n.getRev().hasAuthorDate());
            assertTrue(n.getRev().hasAuthorDateOffset());
            assertTrue(n.getRev().hasCommitterDate());
            assertTrue(n.getRev().hasCommitterDateOffset());

            // FIXME: all the timestamps are one hour off?!
            // System.err.println(revId + " " + n.getRev().getAuthorDate() + " " +
            // n.getRev().getAuthorDateOffset());
            // System.err.println(revId + " " + n.getRev().getCommitterDate() + " " +
            // n.getRev().getCommitterDateOffset());

            // assertEquals(expectedAuthorTimestamps.get(revId), n.getRev().getAuthorDate());
            assertEquals(expectedAuthorTimestampOffsets.get(revId), n.getRev().getAuthorDateOffset());
            // assertEquals(expectedCommitterTimestamps.get(revId), n.getRev().getAuthorDate());
            assertEquals(expectedCommitterTimestampOffsets.get(revId), n.getRev().getAuthorDateOffset());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 19})
    public void testReleases(Integer relId) {
        Map<Integer, String> expectedMessages = Map.of(10, "Version 1.0", 19, "Version 2.0");
        Map<Integer, String> expectedNames = Map.of(10, "v1.0", 19, "v2.0");

        Map<Integer, String> expectedAuthors = Map.of(10, "foo", 19, "bar");

        Map<Integer, Long> expectedAuthorTimestamps = Map.of(10, 1234567890L);
        Map<Integer, Integer> expectedAuthorTimestampOffsets = Map.of(3, 120);

        HashMap<Integer, String> personMapping = new HashMap<>();

        Node n = client.getNode(GetNodeRequest.newBuilder().setSwhid(fakeSWHID("rel", relId).toString()).build());
        assertTrue(n.hasRel());
        assertTrue(n.getRel().hasMessage());
        assertEquals(expectedMessages.get(relId), n.getRel().getMessage().toStringUtf8());
        // FIXME: names are always empty?!
        // System.err.println(relId + " " + n.getRel().getName());
        // assertEquals(expectedNames.get(relId), n.getRel().getName().toStringUtf8());

        // Persons are anonymized, we just need to check that the mapping is self-consistent
        assertTrue(n.getRel().hasAuthor());
        int actualPerson = (int) n.getRel().getAuthor();
        String expectedPerson = expectedAuthors.get(relId);
        assertTrue(actualPerson >= 0);
        if (personMapping.containsKey(actualPerson)) {
            assertEquals(personMapping.get(actualPerson), expectedPerson);
        } else {
            personMapping.put(actualPerson, expectedPerson);
        }

        if (relId == 10) {
            assertTrue(n.getRel().hasAuthorDate());
            assertTrue(n.getRel().hasAuthorDateOffset());
        } else if (relId == 19) {
            assertFalse(n.getRel().hasAuthorDate());
            assertFalse(n.getRel().hasAuthorDateOffset());
        } else {
            assertTrue(false);
        }

        // FIXME: all the timestamps are one hour off?!
        // if (expectedAuthorTimestamps.containsKey(relId)) {
        // assertEquals(expectedAuthorTimestamps.get(revId), n.getRev().getAuthorDate());
        // }
        if (expectedAuthorTimestampOffsets.containsKey(relId)) {
            assertEquals(expectedAuthorTimestampOffsets.get(relId), n.getRev().getAuthorDateOffset());
        }
    }

    @Test
    public void testOrigins() {
        List<SWHID> expectedOris = List.of(new SWHID(TEST_ORIGIN_ID));
        Map<SWHID, String> expectedUrls = Map.of(new SWHID(TEST_ORIGIN_ID), "https://example.com/swh/graph");

        for (SWHID oriSwhid : expectedOris) {
            Node n = client.getNode(GetNodeRequest.newBuilder().setSwhid(oriSwhid.toString()).build());
            assertTrue(n.hasOri());
            assertTrue(n.getOri().hasUrl());
            assertEquals(expectedUrls.get(oriSwhid), n.getOri().getUrl());
        }
    }

    @Test
    public void testCntMask() {
        Node n;
        String swhid = fakeSWHID("cnt", 1).toString();

        // No mask, all fields present
        n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid).build());
        assertTrue(n.hasCnt());
        assertTrue(n.getCnt().hasLength());
        assertEquals(42, n.getCnt().getLength());
        assertTrue(n.getCnt().hasIsSkipped());
        assertFalse(n.getCnt().getIsSkipped());

        // Empty mask, no fields present
        n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid).setMask(FieldMask.getDefaultInstance()).build());
        assertFalse(n.getCnt().hasLength());
        assertFalse(n.getCnt().hasIsSkipped());

        // Mask with length, no isSkipped
        n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid)
                .setMask(FieldMask.newBuilder().addPaths("cnt.length").build()).build());
        assertTrue(n.getCnt().hasLength());
        assertFalse(n.getCnt().hasIsSkipped());

        // Mask with isSkipped, no length
        n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid)
                .setMask(FieldMask.newBuilder().addPaths("cnt.is_skipped").build()).build());
        assertFalse(n.getCnt().hasLength());
        assertTrue(n.getCnt().hasIsSkipped());
    }

    @Test
    public void testRevMask() {
        Node n;
        String swhid = fakeSWHID("rev", 3).toString();

        // No mask, all fields present
        n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid).build());
        assertTrue(n.hasRev());
        assertTrue(n.getRev().hasMessage());
        assertTrue(n.getRev().hasAuthor());
        assertTrue(n.getRev().hasAuthorDate());
        assertTrue(n.getRev().hasAuthorDateOffset());
        assertTrue(n.getRev().hasCommitter());
        assertTrue(n.getRev().hasCommitterDate());
        assertTrue(n.getRev().hasCommitterDateOffset());

        // Empty mask, no fields present
        n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid).setMask(FieldMask.getDefaultInstance()).build());
        assertFalse(n.getRev().hasMessage());
        assertFalse(n.getRev().hasAuthor());
        assertFalse(n.getRev().hasAuthorDate());
        assertFalse(n.getRev().hasAuthorDateOffset());
        assertFalse(n.getRev().hasCommitter());
        assertFalse(n.getRev().hasCommitterDate());
        assertFalse(n.getRev().hasCommitterDateOffset());

        // Test all masks with single fields
        for (Descriptors.FieldDescriptor includedField : RevisionData.getDefaultInstance().getAllFields().keySet()) {
            n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid)
                    .setMask(FieldMask.newBuilder().addPaths("rev." + includedField.getName()).build()).build());
            for (Descriptors.FieldDescriptor f : n.getRev().getDescriptorForType().getFields()) {
                assertEquals(n.getRev().hasField(f), f.getName().equals(includedField.getName()));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 19})
    public void testRelMask(Integer relId) {
        Node n;
        String swhid = fakeSWHID("rel", relId).toString();

        // No mask, all fields present
        n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid).build());
        assertTrue(n.hasRel());
        assertTrue(n.getRel().hasMessage());
        assertTrue(n.getRel().hasAuthor());
        if (relId == 10) {
            assertTrue(n.getRel().hasAuthorDate());
            assertTrue(n.getRel().hasAuthorDateOffset());
        } else if (relId == 19) {
            assertFalse(n.getRel().hasAuthorDate());
            assertFalse(n.getRel().hasAuthorDateOffset());
        } else {
            assertTrue(false);
        }

        // Empty mask, no fields present
        n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid).setMask(FieldMask.getDefaultInstance()).build());
        assertFalse(n.getRel().hasMessage());
        assertFalse(n.getRel().hasAuthor());
        assertFalse(n.getRel().hasAuthorDate());
        assertFalse(n.getRel().hasAuthorDateOffset());

        // Test all masks with single fields
        for (Descriptors.FieldDescriptor includedField : ReleaseData.getDefaultInstance().getAllFields().keySet()) {
            n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid)
                    .setMask(FieldMask.newBuilder().addPaths("rel." + includedField.getName()).build()).build());
            for (Descriptors.FieldDescriptor f : n.getRel().getDescriptorForType().getFields()) {
                assertEquals(n.getRel().hasField(f), f.getName().equals(includedField.getName()));
            }
        }
    }

    @Test
    public void testOriMask() {
        Node n;
        String swhid = TEST_ORIGIN_ID;

        // No mask, all fields present
        n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid).build());
        assertTrue(n.hasOri());
        assertTrue(n.getOri().hasUrl());

        // Empty mask, no fields present
        n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid).setMask(FieldMask.getDefaultInstance()).build());
        assertFalse(n.getOri().hasUrl());

        // Test all masks with single fields
        for (Descriptors.FieldDescriptor includedField : OriginData.getDefaultInstance().getAllFields().keySet()) {
            n = client.getNode(GetNodeRequest.newBuilder().setSwhid(swhid)
                    .setMask(FieldMask.newBuilder().addPaths("ori." + includedField.getName()).build()).build());
            for (Descriptors.FieldDescriptor f : n.getOri().getDescriptorForType().getFields()) {
                assertEquals(n.getOri().hasField(f), f.getName().equals(includedField.getName()));
            }
        }
    }
}
