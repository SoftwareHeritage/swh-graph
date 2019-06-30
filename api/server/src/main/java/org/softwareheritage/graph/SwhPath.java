package org.softwareheritage.graph;

import java.util.ArrayList;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonValue;

import org.softwareheritage.graph.SwhId;

public class SwhPath {
  ArrayList<SwhId> path;

  public SwhPath() {
    this.path = new ArrayList<SwhId>();
  }

  public SwhPath(String ...swhIds) {
    this();
    for (String swhId : swhIds) {
      add(new SwhId(swhId));
    }
  }

  public SwhPath(SwhId ...swhIds) {
    this();
    for (SwhId swhId : swhIds) {
      add(swhId);
    }
  }

  @JsonValue
  public ArrayList<SwhId> getPath() {
    return path;
  }

  public void add(SwhId swhId) {
    path.add(swhId);
  }

  public SwhId get(int index) {
    return path.get(index);
  }

  public int size() {
    return path.size();
  }

  public void reverse() {
    Collections.reverse(path);
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == this) return true;
    if (!(otherObj instanceof SwhPath)) return false;

    SwhPath other = (SwhPath) otherObj;
    if (size() != other.size()) {
      return false;
    }

    for (int i = 0; i < size(); i++) {
      SwhId thisSwhId = get(i);
      SwhId otherSwhId = other.get(i);
      if (!thisSwhId.equals(otherSwhId)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    String str = new String();
    for (SwhId swhId : path) {
      str += swhId + "/";
    }
    return str;
  }
}
