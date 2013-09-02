package cc.bran.tumblr.persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.joda.time.Duration;
import org.joda.time.Instant;

import cc.bran.tumblr.persistence.SqlitePostDb;
import cc.bran.tumblr.types.Post;

import com.google.common.collect.ImmutableSet;

/**
 * Tests for {@link SqlitePostDb}.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class SqlitePostDbTest extends TestCase {

  private static final Post FIRST_POST = new Post(513, "foo.tumblr.com",
          "http://foo.tumblr.com/posts/513/whee", Instant.now().minus(Duration.millis(5000)),
          Instant.now(), ImmutableSet.of("tag1", "tag2", "tag3"), "test post",
          "hello world, this is a test post");

  private static final Post FIRST_POST_EDITED = new Post(513, "foo.tumblr.com",
          "http://foo.tumblr.com/posts/513/whee", Instant.now().minus(Duration.millis(6000)),
          Instant.now().minus(Duration.millis(2100)), ImmutableSet.of("tag2", "tag4"),
          "edited test post", "the old content was bad");

  private static final Post SECOND_POST = new Post(655, "bar.tumblr.com",
          "http://bar.tumblr.com/posts/655/xyzzy", Instant.now().minus(Duration.millis(2332)),
          Instant.now().minus(Duration.millis(122)), ImmutableSet.of("tag2", "tag3", "tag4"),
          "second post", "here is another test post");

  static {
    try {
      Class.forName("org.sqlite.JDBC");
    } catch (ClassNotFoundException exception) {
      throw new AssertionError("org.sqlite.JDBC must be available", exception);
    }
  }

  private SqlitePostDb postDb;

  public SqlitePostDbTest(String testName) {
    super(testName);
  }

  public void setUp() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
    postDb = new SqlitePostDb(connection);
  }

  public void testEdit() throws SQLException {
    postDb.put(FIRST_POST);
    Post retrievedPost = postDb.get(FIRST_POST.getId());
    assertEquals(FIRST_POST, retrievedPost);

    postDb.put(FIRST_POST_EDITED);
    retrievedPost = postDb.get(FIRST_POST.getId());
    assertEquals(FIRST_POST_EDITED, retrievedPost);

    postDb.put(FIRST_POST);
    retrievedPost = postDb.get(FIRST_POST.getId());
    assertEquals(FIRST_POST, retrievedPost);
  }

  public void testGet() throws SQLException {
    postDb.put(FIRST_POST);
    postDb.put(SECOND_POST);

    Post retrievedFirstPost = postDb.get(FIRST_POST.getId());
    Post retrievedSecondPost = postDb.get(SECOND_POST.getId());
    assertEquals(FIRST_POST, retrievedFirstPost);
    assertEquals(SECOND_POST, retrievedSecondPost);
  }

  public void testPut() throws SQLException {
    postDb.put(FIRST_POST);
  }

  public static Test suite() {
    return new TestSuite(SqlitePostDbTest.class);
  }
}
