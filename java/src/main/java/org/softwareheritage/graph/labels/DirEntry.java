package org.softwareheritage.graph.labels;

/**
 * Directory entries metadata are stored as edge labels on the graph. {@link DirEntry} can be encoded in a single long
 * type, to re-use Webgraph interface.
 *
 * @author The Software Heritage developers
 */
public class DirEntry {
    public long filenameId;
    public int permission;

    public DirEntry(long filenameId, int permission)
    {
        this.filenameId = filenameId;
        this.permission = permission;
    }

    public DirEntry(long dirEntryEncoded)
    {
        this.filenameId = dirEntryEncoded >> Permission.NB_BITS_PER_TYPE;
        int dirBytes = (int) (dirEntryEncoded & ((1 << Permission.NB_BITS_PER_TYPE) - 1));
        this.permission = Permission.Type.fromEncoded(dirBytes);
    }

    public long toEncoded()
    {
        return (filenameId << Permission.NB_BITS_PER_TYPE) + Permission.Type.toEncoded(permission);
    }

    public static int labelWidth(long numLabels)
    {
        int filenameIdWidth = (int) Math.ceil(Math.log(numLabels) / Math.log(2));
        if (filenameIdWidth > Long.SIZE - Permission.NB_BITS_PER_TYPE) {
            System.err.println("FIXME: Too many filenames, we can't handle more than 2^" + (Long.SIZE - Permission.NB_BITS_PER_TYPE) + " for now.");
            System.exit(2);
        }
        return filenameIdWidth + Permission.NB_BITS_PER_TYPE;
    }

    /**
     * Permission types present in the Software Heritage graph.
     *
     * @author The Software Heritage developers
     */
    private static class Permission {
        public static final int NB_BITS_PER_TYPE =
                (int) Math.ceil(Math.log(Permission.Type.values().length) / Math.log(2));

        public enum Type {
            CONTENT,
            EXECUTABLE_CONTENT,
            SYMLINK,
            DIRECTORY,
            REVISION;

            public static Permission.Type fromIntCode(int intCode) {
                switch (intCode) {
                    case 0:
                        return CONTENT;
                    case 1:
                        return EXECUTABLE_CONTENT;
                    case 2:
                        return SYMLINK;
                    case 3:
                        return DIRECTORY;
                    case 4:
                        return REVISION;
                }
                throw new IllegalArgumentException("Unknown node permission code: " + intCode);
            }

            public static int toIntCode(Permission.Type type) {
                switch (type) {
                    case CONTENT:
                        return 0;
                    case EXECUTABLE_CONTENT:
                        return 1;
                    case SYMLINK:
                        return 2;
                    case DIRECTORY:
                        return 3;
                    case REVISION:
                        return 4;
                }
                throw new IllegalArgumentException("Unknown node permission type: " + type);
            }

            public static Permission.Type fromIntPerm(int intPerm) {
                switch (intPerm) {
                    case 0100644:
                        return CONTENT;
                    case 0100755:
                        return EXECUTABLE_CONTENT;
                    case 0120000:
                        return SYMLINK;
                    case 0040000:
                        return DIRECTORY;
                    case 0160000:
                        return REVISION;
                }
                throw new IllegalArgumentException("Unknown node permission: " + intPerm);
            }

            public static int toIntPerm(Permission.Type type) {
                switch (type) {
                    case CONTENT:
                        return 0100644;
                    case EXECUTABLE_CONTENT:
                        return 0100755;
                    case SYMLINK:
                        return 0120000;
                    case DIRECTORY:
                        return 0040000;
                    case REVISION:
                        return 0160000;
                }
                throw new IllegalArgumentException("Unknown node permission type: " + type);
            }

            public static int fromEncoded(int encoded) {
                return toIntPerm(fromIntCode(encoded));
            }

            public static int toEncoded(int permission) {
                return toIntCode(fromIntPerm(permission));
            }
        }
    }

}
