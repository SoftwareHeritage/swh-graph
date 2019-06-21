package org.softwareheritage.graph;

import com.fasterxml.jackson.annotation.JsonValue;

public class SwhId {
  public static final int HASH_LENGTH = 40;

  String swhId;
  String type;
  String hash;

  // SWH ID format: 'swh:1:type:hash'
  // https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html
  public SwhId(String swhId) {
    this.swhId = swhId;

    String[] parts = swhId.split(":");
    if (parts.length != 4 || !parts[0].equals("swh") || !parts[1].equals("1")) {
      throw new IllegalArgumentException("Expected SWH ID format to be 'swh:1:type:hash', got: " + swhId);
    }

    this.type = parts[2];
    if (!type.matches("cnt|dir|rel|rev|snp")) {
      throw new IllegalArgumentException("Unknown SWH ID type in: " + swhId);
    }

    this.hash = parts[3];
    if (!hash.matches("[0-9a-f]{" + HASH_LENGTH + "}")) {
      throw new IllegalArgumentException("Wrong SWH ID hash format in: " + swhId);
    }
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == this) return true;
    if (!(otherObj instanceof SwhId)) return false;

    SwhId other = (SwhId) otherObj;
    return swhId.equals(other.getSwhId());
  }

  @Override
  public String toString() {
    return swhId;
  }

  @JsonValue
  public String getSwhId() {
    return swhId;
  }

  public String getType() {
    return type;
  }

  public String getHash() {
    return hash;
  }
}
