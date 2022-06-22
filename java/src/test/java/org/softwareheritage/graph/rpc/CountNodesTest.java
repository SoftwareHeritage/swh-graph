/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.rpc;

import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.softwareheritage.graph.SWHID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CountNodesTest extends TraversalServiceTest {
    private TraversalRequest.Builder getTraversalRequestBuilder(SWHID src) {
        return TraversalRequest.newBuilder().addSrc(src.toString());
    }

    @Test
    public void testSwhidErrors() {
        StatusRuntimeException thrown;
        thrown = assertThrows(StatusRuntimeException.class, () -> client
                .countNodes(TraversalRequest.newBuilder().addSrc(fakeSWHID("cnt", 404).toString()).build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
        thrown = assertThrows(StatusRuntimeException.class, () -> client.countNodes(
                TraversalRequest.newBuilder().addSrc("swh:1:lol:0000000000000000000000000000000000000001").build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
        thrown = assertThrows(StatusRuntimeException.class, () -> client.countNodes(
                TraversalRequest.newBuilder().addSrc("swh:1:cnt:000000000000000000000000000000000000000z").build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void forwardFromRoot() {
        CountResponse actual = client.countNodes(getTraversalRequestBuilder(new SWHID(TEST_ORIGIN_ID)).build());
        assertEquals(12, actual.getCount());
    }

    @Test
    public void forwardFromMiddle() {
        CountResponse actual = client.countNodes(getTraversalRequestBuilder(fakeSWHID("dir", 12)).build());
        assertEquals(8, actual.getCount());
    }

    @Test
    public void forwardRelRev() {
        CountResponse actual = client
                .countNodes(getTraversalRequestBuilder(fakeSWHID("rel", 10)).setEdges("rel:rev,rev:rev").build());
        assertEquals(3, actual.getCount());
    }

    @Test
    public void backwardFromMiddle() {
        CountResponse actual = client.countNodes(
                getTraversalRequestBuilder(fakeSWHID("dir", 12)).setDirection(GraphDirection.BACKWARD).build());
        assertEquals(4, actual.getCount());
    }

    @Test
    public void backwardFromLeaf() {
        CountResponse actual = client.countNodes(
                getTraversalRequestBuilder(fakeSWHID("cnt", 4)).setDirection(GraphDirection.BACKWARD).build());
        assertEquals(11, actual.getCount());
    }

    @Test
    public void backwardRevToRevRevToRel() {
        CountResponse actual = client.countNodes(getTraversalRequestBuilder(fakeSWHID("rev", 3))
                .setEdges("rev:rev,rev:rel").setDirection(GraphDirection.BACKWARD).build());
        assertEquals(6, actual.getCount());
    }

    @Test
    public void testWithEmptyMask() {
        CountResponse actual = client.countNodes(
                getTraversalRequestBuilder(fakeSWHID("dir", 12)).setMask(FieldMask.getDefaultInstance()).build());
        assertEquals(8, actual.getCount());
    }
}
