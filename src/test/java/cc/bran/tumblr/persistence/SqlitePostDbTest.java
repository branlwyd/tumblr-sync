package cc.bran.tumblr.persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.joda.time.Duration;
import org.joda.time.Instant;

import cc.bran.tumblr.types.AnswerPost;
import cc.bran.tumblr.types.AudioPost;
import cc.bran.tumblr.types.ChatPost;
import cc.bran.tumblr.types.ChatPost.Dialogue;
import cc.bran.tumblr.types.LinkPost;
import cc.bran.tumblr.types.Post;
import cc.bran.tumblr.types.QuotePost;
import cc.bran.tumblr.types.TextPost;
import cc.bran.tumblr.types.VideoPost;
import cc.bran.tumblr.types.VideoPost.Video;

import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link SqlitePostDb}.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class SqlitePostDbTest extends TestCase {

  private static final Post ANSWER_POST_1 = new AnswerPost(566, "foo.tumblr.com",
          "http://foo.tumblr.com/posts/566/whee", Instant.now().minus(Duration.millis(5100)),
          Instant.now().minus(Duration.millis(2200)), ImmutableList.of("tag1", "tag4"), "fool",
          "http://fool.tumblr.com/", "War, huh, what is it good for?", "Absolutely nothing.");

  private static final Post ANSWER_POST_1_EDITED = new AnswerPost(566, "foo.tumblr.com",
          "http://foo.tumblr.com/posts/566/whee", Instant.now().minus(Duration.millis(5100)),
          Instant.now().minus(Duration.millis(2200)), ImmutableList.of("tag1", "tag4"), "fool",
          "http://fool.tumblr.com/", "War, huh, what is it good for?", "Oil, lol.");

  private static final Post AUDIO_POST_1 = new AudioPost(414, "foo.tumblr.com",
          "http://foo.tumblr.com/414/whee", Instant.now().minus(Duration.millis(5200)),
          Instant.now(), ImmutableList.of("tag2", "tag5"), "a song", "player", 52, "album art",
          "fartist", "fartistry", "a track name", 2, 1993);

  private static final Post AUDIO_POST_1_EDITED = new AudioPost(414, "foo.tumblr.com",
          "http://foo.tumblr.com/414/whee", Instant.now().minus(Duration.millis(5200)),
          Instant.now(), ImmutableList.of("tag4", "tag5"), "edited song", "player", 52,
          "album art", "artist formerly known as fartist", "fartistry x 2", "a track name", 2, 1993);

  private static final Post CHAT_POST_1 = new ChatPost(123, "foo.tumblr.com",
          "http://foo.tumblr.com/123/whee", Instant.now().minus(Duration.millis(1234)),
          Instant.now(), ImmutableList.of("tag2", "tag5"), "chat title", "chat body",
          ImmutableList.of(new Dialogue("person 1", "first", "hello"), new Dialogue("person 2",
                  "second", "hi")));

  private static final Post CHAT_POST_1_EDITED = new ChatPost(123, "foo.tumblr.com",
          "http://foo.tumblr.com/123/whee", Instant.now().minus(Duration.millis(1234)),
          Instant.now(), ImmutableList.of("tag2", "tag5"), "chat title", "chat body",
          ImmutableList.of(new Dialogue("person 1", "first", "hello"), new Dialogue("person 2",
                  "second", "hi"), new Dialogue("person 1", "third", "how are you?")));

  private static final Post LINK_POST_1 = new LinkPost(4003, "foo.tumblr.com",
          "http://foo.tumblr.com/4003/whee", Instant.now(), Instant.now(), ImmutableList.of("tag5",
                  "tag6"), "title", "url", "description");

  private static final Post LINK_POST_1_EDITED = new LinkPost(4003, "foo.tumblr.com",
          "http://foo.tumblr.com/4003/whee", Instant.now(), Instant.now(), ImmutableList.of("tag5",
                  "tag6"), "new-title", "new-url", "new-description");

  private static final Post QUOTE_POST_1 = new QuotePost(3324, "foo.tumblr.com",
          "http://foo.tumblr.com/posts/3324/whee", Instant.now().minus(Duration.millis(332)),
          Instant.now(), ImmutableList.of("tag1", "tag2", "tag5"),
          "a fool and his money are soon parted", "danny de vito");

  private static final Post QUOTE_POST_1_EDITED = new QuotePost(3324, "foo.tumblr.com",
          "http://foo.tumblr.com/posts/3324/whee", Instant.now().minus(Duration.millis(332)),
          Instant.now(), ImmutableList.of("tag1", "tag2", "tag5"), "oi u wat mate", "danny de vito");

  private static final Post TEXT_POST_1 = new TextPost(513, "foo.tumblr.com",
          "http://foo.tumblr.com/posts/513/whee", Instant.now().minus(Duration.millis(5000)),
          Instant.now(), ImmutableList.of("tag1", "tag2", "tag3"), "test post",
          "hello world, this is a test post");

  private static final Post TEXT_POST_1_EDITED = new TextPost(513, "foo.tumblr.com",
          "http://foo.tumblr.com/posts/513/whee", Instant.now().minus(Duration.millis(6000)),
          Instant.now().minus(Duration.millis(2100)), ImmutableList.of("tag2", "tag4"),
          "edited test post", "the old content was bad");

  private static final Post VIDEO_POST_1 = new VideoPost(9985, "foo.tumblr.com",
          "http://foo.tumblr.com/9985/whee", Instant.now().minus(Duration.millis(101)),
          Instant.now(), ImmutableList.of("tag1", "tag5"), "caption", ImmutableList.of(new Video(
                  200, "embed200"), new Video(400, "embed400")));

  private static final Post VIDEO_POST_1_EDITED = new VideoPost(9985, "foo.tumblr.com",
          "http://foo.tumblr.com/9985/whee", Instant.now().minus(Duration.millis(101)),
          Instant.now(), ImmutableList.of("tag1", "tag5"), "new caption", ImmutableList.of(
                  new Video(200, "embed200"), new Video(400, "embed400"),
                  new Video(600, "embed600")));

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

  public void assertCanDelete(Post post) throws SQLException {
    postDb.put(post);
    assertNotNull(postDb.get(post.getId()));
    postDb.delete(post.getId());
    assertNull(postDb.get(post.getId()));
  }

  public void assertCanEdit(Post post, Post editedPost) throws SQLException {
    assertEquals(post.getId(), editedPost.getId());
    assertFalse(post.equals(editedPost));

    postDb.put(post);
    Post retrievedPost = postDb.get(post.getId());
    assertEquals(post, retrievedPost);

    postDb.put(editedPost);
    retrievedPost = postDb.get(post.getId());
    assertEquals(editedPost, retrievedPost);

    postDb.put(post);
    retrievedPost = postDb.get(post.getId());
    assertEquals(post, retrievedPost);
  }

  public void assertCanGet(Post post) throws SQLException {
    postDb.put(post);
    Post retrievedPost = postDb.get(post.getId());
    assertEquals(post, retrievedPost);
    assertNotSame(post, retrievedPost);
  }

  public void assertCanPut(Post post) throws SQLException {
    postDb.put(post);
  }

  public void setUp() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
    postDb = new SqlitePostDb(connection);
  }

  public void tearDown() throws SQLException {
    postDb.close();
  }

  public void testDelete_answerPost() throws SQLException {
    assertCanDelete(ANSWER_POST_1);
  }

  public void testDelete_audioPost() throws SQLException {
    assertCanDelete(AUDIO_POST_1);
  }

  public void testDelete_chatPost() throws SQLException {
    assertCanDelete(CHAT_POST_1);
  }

  public void testDelete_linkPost() throws SQLException {
    assertCanDelete(LINK_POST_1);
  }

  public void testDelete_quotePost() throws SQLException {
    assertCanDelete(QUOTE_POST_1);
  }

  public void testDelete_textPost() throws SQLException {
    assertCanDelete(TEXT_POST_1);
  }

  public void testDelete_videoPost() throws SQLException {
    assertCanDelete(VIDEO_POST_1);
  }

  public void testEdit_answerPost() throws SQLException {
    assertCanEdit(ANSWER_POST_1, ANSWER_POST_1_EDITED);
  }

  public void testEdit_audioPost() throws SQLException {
    assertCanEdit(AUDIO_POST_1, AUDIO_POST_1_EDITED);
  }

  public void testEdit_chatPost() throws SQLException {
    assertCanEdit(CHAT_POST_1, CHAT_POST_1_EDITED);
  }

  public void testEdit_linkPost() throws SQLException {
    assertCanEdit(LINK_POST_1, LINK_POST_1_EDITED);
  }

  public void testEdit_quotePost() throws SQLException {
    assertCanEdit(QUOTE_POST_1, QUOTE_POST_1_EDITED);
  }

  public void testEdit_textPost() throws SQLException {
    assertCanEdit(TEXT_POST_1, TEXT_POST_1_EDITED);
  }

  public void testEdit_videoPost() throws SQLException {
    assertCanEdit(VIDEO_POST_1, VIDEO_POST_1_EDITED);
  }

  public void testGet_answerPost() throws SQLException {
    assertCanGet(ANSWER_POST_1);
  }

  public void testGet_audioPost() throws SQLException {
    assertCanGet(AUDIO_POST_1);
  }

  public void testGet_chatPost() throws SQLException {
    assertCanGet(CHAT_POST_1);
  }

  public void testGet_linkPost() throws SQLException {
    assertCanGet(LINK_POST_1);
  }

  public void testGet_nonexistent() throws SQLException {
    assertNull(postDb.get(12345));
  }

  public void testGet_quotePost() throws SQLException {
    assertCanGet(QUOTE_POST_1);
  }

  public void testGet_textPost() throws SQLException {
    assertCanGet(TEXT_POST_1);
  }

  public void testGet_videoPost() throws SQLException {
    assertCanGet(VIDEO_POST_1);
  }

  public void testPut_answerPost() throws SQLException {
    assertCanPut(ANSWER_POST_1);
  }

  public void testPut_audioPost() throws SQLException {
    assertCanPut(AUDIO_POST_1);
  }

  public void testPut_chatPost() throws SQLException {
    assertCanPut(CHAT_POST_1);
  }

  public void testPut_linkPost() throws SQLException {
    assertCanPut(LINK_POST_1);
  }

  public void testPut_quotePost() throws SQLException {
    assertCanPut(QUOTE_POST_1);
  }

  public void testPut_textPost() throws SQLException {
    assertCanPut(TEXT_POST_1);
  }

  public void testPut_videoPost() throws SQLException {
    assertCanPut(VIDEO_POST_1);
  }

  public static Test suite() {
    return new TestSuite(SqlitePostDbTest.class);
  }
}
