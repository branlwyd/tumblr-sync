package cc.bran.util;

import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * Represents a union (Either) type. Instances of this class have one value,
 * which may be of one of two types.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 * 
 * @param <A>
 *          the left type
 * @param <B>
 *          the right type
 */
public abstract class Either<A, B> {

  private static class Left<A, B> extends Either<A, B> {

    private final A value;

    private Left(A value) {
      Preconditions.checkNotNull(value);
      this.value = value;
    }

    @Override
    public A getLeft() {
      return value;
    }

    @Override
    public B getRight() {
      return null;
    }

    @Override
    public ValueType getValueType() {
      return ValueType.LEFT;
    }
  }

  private static class Right<A, B> extends Either<A, B> {

    private final B value;

    private Right(B value) {
      Preconditions.checkNotNull(value);
      this.value = value;
    }

    @Override
    public A getLeft() {
      return null;
    }

    @Override
    public B getRight() {
      return value;
    }

    @Override
    public ValueType getValueType() {
      return ValueType.RIGHT;
    }
  }

  public enum ValueType {
    LEFT, RIGHT
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Either<?, ?>)) {
      return false;
    }
    Either<?, ?> otherEither = (Either<?, ?>) other;
    return Objects.equals(this.getLeft(), otherEither.getLeft())
            && Objects.equals(this.getRight(), otherEither.getRight());
  }

  public abstract A getLeft();

  public abstract B getRight();

  public abstract ValueType getValueType();

  @Override
  public int hashCode() {
    return Objects.hash(getLeft(), getRight());
  }

  public boolean hasLeft() {
    return (getValueType() == ValueType.LEFT);
  }

  public boolean hasRight() {
    return (getValueType() == ValueType.RIGHT);
  }

  public static <A, B> Either<A, B> fromLeft(A value) {
    return new Left<>(value);
  }

  public static <A, B> Either<A, B> fromRight(B value) {
    return new Right<>(value);
  }
}
