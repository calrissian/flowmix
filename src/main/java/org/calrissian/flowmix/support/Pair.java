package org.calrissian.flowmix.support;


import java.io.Serializable;

public class Pair<O,T> implements Serializable {

  private O one;
  private T two;

  public Pair(O one, T two) {
    this.one = one;
    this.two = two;
  }

  public O getOne() {
    return one;
  }

  public T getTwo() {
    return two;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Pair pair = (Pair) o;

    if (one != null ? !one.equals(pair.one) : pair.one != null) return false;
    if (two != null ? !two.equals(pair.two) : pair.two != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = one != null ? one.hashCode() : 0;
    result = 31 * result + (two != null ? two.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Pair{" +
            "one=" + one +
            ", two=" + two +
            '}';
  }
}
