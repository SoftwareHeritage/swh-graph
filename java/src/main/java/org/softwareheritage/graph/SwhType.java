/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Software Heritage graph node types, as described in the
 * <a href="https://docs.softwareheritage.org/devel/swh-model/data-model.html">data model</a>.
 */
public enum SwhType {
    /** Content node */
    CNT,
    /** Directory node */
    DIR,
    /** Origin node */
    ORI,
    /** Release node */
    REL,
    /** Revision node */
    REV,
    /** Snapshot node */
    SNP;

    /**
     * Converts integer to corresponding SWH node type.
     *
     * @param intType node type represented as an integer
     * @return the corresponding {@link SwhType} value
     * @see SwhType
     */
    public static SwhType fromInt(int intType) {
        switch (intType) {
            case 0:
                return CNT;
            case 1:
                return DIR;
            case 2:
                return ORI;
            case 3:
                return REL;
            case 4:
                return REV;
            case 5:
                return SNP;
        }
        return null;
    }

    /**
     * Converts node types to the corresponding int value
     *
     * @param type node type as an enum
     * @return the corresponding int value
     */
    public static int toInt(SwhType type) {
        switch (type) {
            case CNT:
                return 0;
            case DIR:
                return 1;
            case ORI:
                return 2;
            case REL:
                return 3;
            case REV:
                return 4;
            case SNP:
                return 5;
        }
        throw new IllegalArgumentException("Unknown node type: " + type);
    }

    /**
     * Converts string to corresponding SWH node type.
     *
     * @param strType node type represented as a string
     * @return the corresponding {@link SwhType} value
     * @see SwhType
     */
    public static SwhType fromStr(String strType) {
        if (!strType.matches("cnt|dir|ori|rel|rev|snp")) {
            throw new IllegalArgumentException("Unknown node type: " + strType);
        }
        return SwhType.valueOf(strType.toUpperCase());
    }

    /**
     * Converts byte array name to the int code of the corresponding SWH node type. Used for
     * performance-critical deserialization.
     *
     * @param name node type represented as a byte array (e.g. b"cnt")
     * @return the ordinal value of the corresponding {@link SwhType}
     * @see SwhType
     */
    public static int byteNameToInt(byte[] name) {
        if (Arrays.equals(name, "cnt".getBytes())) {
            return 0;
        } else if (Arrays.equals(name, "dir".getBytes())) {
            return 1;
        } else if (Arrays.equals(name, "ori".getBytes())) {
            return 2;
        } else if (Arrays.equals(name, "rel".getBytes())) {
            return 3;
        } else if (Arrays.equals(name, "rev".getBytes())) {
            return 4;
        } else if (Arrays.equals(name, "snp".getBytes())) {
            return 5;
        } else
            return -1;
    }

    /**
     * Parses SWH node type possible values from formatted string (see the
     * <a href="https://docs.softwareheritage.org/devel/swh-graph/api.html#terminology">API syntax</a>).
     *
     * @param strFmtType node types represented as a formatted string
     * @return a list containing the {@link SwhType} values
     * @see SwhType
     */
    public static ArrayList<SwhType> parse(String strFmtType) {
        ArrayList<SwhType> types = new ArrayList<>();

        if (strFmtType.equals("*")) {
            List<SwhType> nodeTypes = Arrays.asList(SwhType.values());
            types.addAll(nodeTypes);
        } else {
            types.add(SwhType.fromStr(strFmtType));
        }

        return types;
    }
}
