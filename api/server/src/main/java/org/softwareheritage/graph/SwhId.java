package org.softwareheritage.graph;

import com.fasterxml.jackson.annotation.JsonValue;

public class SwhId {
  String swhId;
  String type;
  String hash;

  public SwhId(String swhId) {
    this.swhId = swhId;
    String[] parts = swhId.split(":");
    if (parts.length != 4) {
      throw new IllegalArgumentException("Incorrect SWH ID format: " + swhId);
    }

    // SWH ID format: 'swh:1:type:hash'
    // https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html
    this.type = parts[2];
    this.hash = parts[3];
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == this) return true;
    if (! (otherObj instanceof SwhId)) return false;

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
