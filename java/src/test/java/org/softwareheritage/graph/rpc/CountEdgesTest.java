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

public class CountEdgesTest extends TraversalServiceTest {
    private TraversalRequest.Builder getTraversalRequestBuilder(SWHID src) {
        return TraversalRequest.newBuilder().addSrc(src.toString());
    }

    @Test
    public void testSwhidErrors() {
        StatusRuntimeException thrown;
        thrown = assertThrows(StatusRuntimeException.class, () -> client
                .countEdges(TraversalRequest.newBuilder().addSrc(fakeSWHID("cnt", 404).toString()).build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
        thrown = assertThrows(StatusRuntimeException.class, () -> client.countEdges(
                TraversalRequest.newBuilder().addSrc("swh:1:lol:0000000000000000000000000000000000000001").build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
        thrown = assertThrows(StatusRuntimeException.class, () -> client.countEdges(
                TraversalRequest.newBuilder().addSrc("swh:1:cnt:000000000000000000000000000000000000000z").build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void forwardFromRoot() {
        CountResponse actual = client.countEdges(getTraversalRequestBuilder(new SWHID(TEST_ORIGIN_ID)).build());
        assertEquals(13, actual.getCount());
    }

    @Test
    public void forwardFromMiddle() {
        CountResponse actual = client.countEdges(getTraversalRequestBuilder(fakeSWHID("dir", 12)).build());
        assertEquals(7, actual.getCount());
    }

    @Test
    public void forwardRelRev() {
        CountResponse actual = client
                .countEdges(getTraversalRequestBuilder(fakeSWHID("rel", 10)).setEdges("rel:rev,rev:rev").build());
        assertEquals(2, actual.getCount());
    }

    @Test
    public void backwardFromMiddle() {
        CountResponse actual = client.countEdges(
                getTraversalRequestBuilder(fakeSWHID("dir", 12)).setDirection(GraphDirection.BACKWARD).build());
        assertEquals(3, actual.getCount());
    }

    @Test
    public void backwardFromLeaf() {
        CountResponse actual = client.countEdges(
                getTraversalRequestBuilder(fakeSWHID("cnt", 4)).setDirection(GraphDirection.BACKWARD).build());
        assertEquals(12, actual.getCount());
    }

    @Test
    public void backwardRevToRevRevToRel() {
        CountResponse actual = client.countEdges(getTraversalRequestBuilder(fakeSWHID("rev", 3))
                .setEdges("rev:rev,rev:rel").setDirection(GraphDirection.BACKWARD).build());
        assertEquals(5, actual.getCount());
    }

    @Test
    public void testWithEmptyMask() {
        CountResponse actual = client.countEdges(
                getTraversalRequestBuilder(fakeSWHID("dir", 12)).setMask(FieldMask.getDefaultInstance()).build());
        assertEquals(7, actual.getCount());
    }
}
