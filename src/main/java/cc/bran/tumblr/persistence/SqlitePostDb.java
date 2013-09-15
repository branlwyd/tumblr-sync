package cc.bran.tumblr.persistence;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.Instant;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import cc.bran.tumblr.types.AnswerPost;
import cc.bran.tumblr.types.AudioPost;
import cc.bran.tumblr.types.ChatPost;
import cc.bran.tumblr.types.LinkPost;
import cc.bran.tumblr.types.PhotoPost;
import cc.bran.tumblr.types.Post;
import cc.bran.tumblr.types.PostType;
import cc.bran.tumblr.types.QuotePost;
import cc.bran.tumblr.types.TextPost;
import cc.bran.tumblr.types.VideoPost;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Persists {@link Post}s using an SQLite backend.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
// TODO(bpitman): don't re-prepare statements with every call to get()/post()/etc
public class SqlitePostDb implements PostDb, AutoCloseable {

  /**
   * Represents a transaction that can be executed.
   * 
   * @author Brandon Pitman (brandon.pitman@gmail.com)
   * @param <E>
   *          the type that is returned from the transaction
   * @param <Ex>
   *          the type that is thrown from the transaction
   */
  private abstract class Transaction<E, Ex extends Exception> {

    /**
     * Executes the transaction, committing if the code returns without throwing an exception, and
     * rolling back if the function throws an exception.
     * 
     * If the attempt to rollback throws an exception, it will be included in the suppressed
     * exceptions for the thrown exception.
     * 
     * @return the value that is returned from the transaction code
     * @throws Ex
     *           if the transaction code throws an exception
     * @throws SQLException
     *           if the attempt to commit throws an exception
     */
    public E execute() throws Ex, SQLException {
      E result;

      try {
        result = runTransaction();
        connection.commit();
      } catch (Exception exception) {
        try {
          connection.rollback();
        } catch (SQLException sqlException) {
          exception.addSuppressed(sqlException);
        }
        throw exception;
      }

      return result;
    }

    /**
     * Runs the code in the transaction. This code can assume that it is inside of a transaction.
     */
    abstract E runTransaction() throws Ex;
  }

  private static final String POST_REQUEST_SQL = "SELECT posts.id, posts.blogName, posts.postUrl, posts.postedTimestamp, posts.retrievedTimestamp, postTypes.type FROM posts JOIN postTypes ON posts.postTypeId = postTypes.id WHERE posts.id = ?;";

  private static final String TAGS_REQUEST_SQL_TEMPLATE = "SELECT postTags.postId, tags.tag FROM postTags JOIN tags ON postTags.tagId = tags.id WHERE postTags.postId IN (%s) ORDER BY postTags.tagIndex;";

  private static final String TEXT_POST_REQUEST_SQL_TEMPLATE = "SELECT id, title, body FROM textPosts WHERE id IN (%s);";

  static {
    try {
      Class.forName("org.sqlite.JDBC");
    } catch (ClassNotFoundException exception) {
      throw new AssertionError("org.sqlite.JDBC must be available", exception);
    }
  }

  private final Connection connection;

  private final PreparedStatement postRequestStatement;

  @VisibleForTesting
  SqlitePostDb(Connection connection) throws SQLException {
    this.connection = connection;
    initConnection();

    postRequestStatement = connection.prepareStatement(POST_REQUEST_SQL);
  }

  public SqlitePostDb(String dbFile) throws ClassNotFoundException, SQLException {
    this(DriverManager.getConnection(String.format("jdbc:sqlite:%s", new File(dbFile).getPath())));
  }

  @Override
  public void close() throws Exception {
    postRequestStatement.close();
  }

  private Post doGet(long id) throws SQLException {
    postRequestStatement.setLong(1, id);
    try (ResultSet resultSet = postRequestStatement.executeQuery()) {
      List<Post> postList = doGetFromResultSet(resultSet);

      if (postList.isEmpty()) {
        return null;
      }

      return postList.get(0);
    }
  }

  private List<Post> doGetFromResultSet(ResultSet resultSet) throws SQLException {
    Map<Long, Post.Builder> builderById = new HashMap<>();
    Map<Long, TextPost.Builder> textBuilderById = new HashMap<>();

    // Extract basic data from results & categorize them by type.
    while (resultSet.next()) {
      Post.Builder postBuilder;
      long id = resultSet.getLong("id");
      PostType postType = PostType.valueOf(resultSet.getString("type"));

      switch (postType) {
      case ANSWER:
        postBuilder = new AnswerPost.Builder();
        throw new NotImplementedException();
      case AUDIO:
        postBuilder = new AudioPost.Builder();
        throw new NotImplementedException();
      case CHAT:
        postBuilder = new ChatPost.Builder();
        throw new NotImplementedException();
      case LINK:
        postBuilder = new LinkPost.Builder();
        throw new NotImplementedException();
      case PHOTO:
        postBuilder = new PhotoPost.Builder();
        throw new NotImplementedException();
      case QUOTE:
        postBuilder = new QuotePost.Builder();
        throw new NotImplementedException();
      case TEXT:
        postBuilder = new TextPost.Builder();
        textBuilderById.put(id, (TextPost.Builder) postBuilder);
        break;
      case VIDEO:
        postBuilder = new VideoPost.Builder();
        throw new NotImplementedException();
      default:
        throw new AssertionError(String.format("Post %d has impossible type %s.", id,
                postType.toString()));
      }

      // Set basic data.
      postBuilder.setId(id);
      postBuilder.setBlogName(resultSet.getString("blogName"));
      postBuilder.setPostUrl(resultSet.getString("postUrl"));
      postBuilder.setPostedInstant(new Instant(resultSet.getLong("postedTimestamp")));
      postBuilder.setRetrievedInstant(new Instant(resultSet.getLong("retrievedTimestamp")));

      builderById.put(id, postBuilder);
    }

    // Set tag data & post type-specific data.
    doGetTagData(builderById);
    doGetTextPostData(textBuilderById);

    // Build result.
    ImmutableList.Builder<Post> resultBuilder = ImmutableList.builder();
    for (Post.Builder postBuilder : builderById.values()) {
      resultBuilder.add(postBuilder.build());
    }
    return resultBuilder.build();
  }

  private void doGetTagData(Map<Long, Post.Builder> builderById) throws SQLException {
    if (builderById.isEmpty()) {
      return;
    }

    // Prepare data structures.
    Map<Long, ImmutableList.Builder<String>> tagListBuilderById = new HashMap<>();
    for (long id : builderById.keySet()) {
      tagListBuilderById.put(id, new ImmutableList.Builder<String>());
    }

    // Request tags & parse data into structure.
    String tagRequestSql = String.format(TAGS_REQUEST_SQL_TEMPLATE,
            buildInQuery(builderById.size()));
    try (PreparedStatement tagRequestStatement = connection.prepareStatement(tagRequestSql)) {
      int index = 1;
      for (long id : builderById.keySet()) {
        tagRequestStatement.setLong(index++, id);
      }

      try (ResultSet resultSet = tagRequestStatement.executeQuery()) {
        while (resultSet.next()) {
          long id = resultSet.getLong("postId");
          String tag = resultSet.getString("tag");

          tagListBuilderById.get(id).add(tag);
        }
      }
    }

    // Place parsed tags into post builders.
    for (Map.Entry<Long, Post.Builder> entry : builderById.entrySet()) {
      long id = entry.getKey();
      Post.Builder postBuilder = entry.getValue();

      postBuilder.setTags(tagListBuilderById.get(id).build());
    }
  }

  private void doGetTextPostData(Map<Long, TextPost.Builder> builderById) throws SQLException {
    if (builderById.isEmpty()) {
      return;
    }

    // Request text post data & place into post builders.
    String textPostRequestSql = String.format(TEXT_POST_REQUEST_SQL_TEMPLATE,
            buildInQuery(builderById.size()));
    try (PreparedStatement textPostRequestStatement = connection
            .prepareStatement(textPostRequestSql)) {
      int index = 1;
      for (long id : builderById.keySet()) {
        textPostRequestStatement.setLong(index++, id);
      }

      try (ResultSet resultSet = textPostRequestStatement.executeQuery()) {
        while (resultSet.next()) {
          long id = resultSet.getLong("id");
          TextPost.Builder builder = builderById.get(id);
          builder.setTitle(resultSet.getString("title"));
          builder.setBody(resultSet.getString("body"));
        }
      }
    }
  }

  private void doPut(Post post) throws SQLException {
    try (PreparedStatement postsInsertStatement = connection
            .prepareStatement("INSERT OR REPLACE INTO posts (id, blogName, postUrl, postedTimestamp, retrievedTimestamp, postTypeId) SELECT ?, ?, ?, ?, ?, id FROM postTypes WHERE type = ?;");
            PreparedStatement textPostsInsertStatement = connection
                    .prepareStatement("INSERT OR REPLACE INTO textPosts (id, title, body) VALUES (?, ?, ?);");
            PreparedStatement postTagsDeleteStatement = connection
                    .prepareStatement("DELETE FROM postTags WHERE postId = ?;");
            PreparedStatement tagsSelectStatement = connection
                    .prepareStatement(buildTagSelectSql(post.getTags().size()));
            PreparedStatement tagsInsertStatement = connection
                    .prepareStatement("INSERT INTO tags (tag) VALUES (?);");
            PreparedStatement postTagsInsertStatement = connection
                    .prepareStatement("INSERT INTO postTags (postId, tagId, tagIndex) VALUES (?, ?, ?);")) {
      TextPost textPost = (TextPost) post;

      // Update posts table.
      postsInsertStatement.setLong(1, textPost.getId());
      postsInsertStatement.setString(2, textPost.getBlogName());
      postsInsertStatement.setString(3, textPost.getPostUrl());
      postsInsertStatement.setLong(4, textPost.getPostedInstant().getMillis());
      postsInsertStatement.setLong(5, textPost.getRetrievedInstant().getMillis());
      postsInsertStatement.setString(6, textPost.getType().toString());
      postsInsertStatement.execute();

      // Update textPosts table.
      textPostsInsertStatement.setLong(1, textPost.getId());
      textPostsInsertStatement.setString(2, textPost.getTitle());
      textPostsInsertStatement.setString(3, textPost.getBody());
      textPostsInsertStatement.execute();

      // Remove old entries from postTags table.
      postTagsDeleteStatement.setLong(1, textPost.getId());
      postTagsDeleteStatement.execute();

      if (!textPost.getTags().isEmpty()) {
        // Look up existing tags.
        int index = 1;
        for (String tag : textPost.getTags()) {
          tagsSelectStatement.setString(index++, tag);
        }
        Map<String, Integer> tagIdByTag = new HashMap<>();
        try (ResultSet resultSet = tagsSelectStatement.executeQuery()) {
          while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String tag = resultSet.getString("tag");

            tagIdByTag.put(tag, id);
          }
        }

        // Create missing tags, if any.
        for (String tag : textPost.getTags()) {
          if (tagIdByTag.containsKey(tag)) {
            continue;
          }

          tagsInsertStatement.setString(1, tag);
          tagsInsertStatement.execute();
          try (ResultSet resultSet = tagsInsertStatement.getGeneratedKeys()) {
            int id = resultSet.getInt(1);
            tagIdByTag.put(tag, id);
          }
        }

        // Insert new entries into postTags table.
        index = 0;
        for (String tag : textPost.getTags()) {
          postTagsInsertStatement.setLong(1, textPost.getId());
          postTagsInsertStatement.setInt(2, tagIdByTag.get(tag));
          postTagsInsertStatement.setInt(3, index++);
          postTagsInsertStatement.addBatch();
        }
        postTagsInsertStatement.executeBatch();
      }
    }
  }

  @Override
  public Post get(final long id) throws SQLException {
    return new Transaction<Post, SQLException>() {

      @Override
      Post runTransaction() throws SQLException {
        return doGet(id);
      }
    }.execute();
  }

  private void initConnection() throws SQLException {
    connection.setAutoCommit(false);

    new Transaction<Void, SQLException>() {

      @Override
      Void runTransaction() throws SQLException {
        try (Statement statement = connection.createStatement()) {
          statement.execute("PRAGMA foreign_keys = ON;");

          // Main post tables.
          statement
                  .execute("CREATE TABLE IF NOT EXISTS posts(id INTEGER PRIMARY KEY, blogName TEXT NOT NULL, postUrl TEXT NOT NULL, postedTimestamp INTEGER NOT NULL, retrievedTimestamp INTEGER NOT NULL, postTypeId INTEGER NOT NULL REFERENCES postTypes(id));");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS textPosts(id INTEGER PRIMARY KEY REFERENCES posts(id), title TEXT NOT NULL, body TEXT NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS photoPosts(id INTEGER PRIMARY KEY REFERENCES posts(id), caption TEXT NOT NULL, width INTEGER NOT NULL, height INTEGER NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS quotePosts(id INTEGER PRIMARY KEY REFERENCES posts(id), text TEXT NOT NULL, source TEXT NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS linkPosts(id INTEGER PRIMARY KEY REFERENCES posts(id), title TEXT NOT NULL, url TEXT NOT NULL, description TEXT NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS chatPosts(id INTEGER PRIMARY KEY REFERENCES posts(id), title TEXT NOT NULL, body TEXT NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS audioPosts(id INTEGER PRIMARY KEY REFERENCES posts(id), caption TEXT NOT NULL, player TEXT NOT NULL, plays INTEGER NOT NULL, albumArt TEXT NOT NULL, artist TEXT NOT NULL, album TEXT NOT NULL, trackName TEXT NOT NULL, trackNumber INTEGER NOT NULL, year INTEGER NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS videoPosts(id INTEGER PRIMARY KEY REFERENCES posts(id), caption TEXT NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS answerPosts(id INTEGER PRIMARY KEY REFERENCES posts(id), askingName TEXT NOT NULL, askingUrl TEXT NOT NULL, question TEXT NOT NULL, answer TEXT NOT NULL);");

          // Tags tables.
          statement
                  .execute("CREATE TABLE IF NOT EXISTS tags(id INTEGER PRIMARY KEY AUTOINCREMENT, tag TEXT UNIQUE NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS postTags(postId INTEGER NOT NULL REFERENCES posts(id), tagId INTEGER NOT NULL REFERENCES tags(id), tagIndex INTEGER NOT NULL, PRIMARY KEY(postId, tagId));");

          // Photo post-specific tables.
          statement
                  .execute("CREATE TABLE IF NOT EXISTS photos(id INTEGER PRIMARY KEY AUTOINCREMENT, caption TEXT NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS photoSizes(id INTEGER PRIMARY KEY AUTOINCREMENT, width INTEGER NOT NULL, height INTEGER NOT NULL, url TEXT NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS photoPostPhotos(postId INTEGER NOT NULL REFERENCES photoPosts(id), photoId INTEGER NOT NULL REFERENCES photos(id), photoIndex INTEGER NOT NULL, PRIMARY KEY(postId, photoId));");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS photoPhotoSizes(photoId INTEGER NOT NULL REFERENCES photos(id), photoSizeId INTEGER NOT NULL REFERENCES photoSizes(id), photoSizeIndex INTEGER NOT NULL, PRIMARY KEY(photoId, photoSizeId));");

          // Chat post-specific tables.
          statement
                  .execute("CREATE TABLE IF NOT EXISTS dialogue(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, label TEXT NOT NULL, phrase TEXT NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS chatPostDialogue(postId INTEGER NOT NULL REFERENCES chatPosts(id), dialogueId INTEGER NOT NULL REFERENCES dialogue(id), dialogueIndex INTEGER NOT NULL, PRIMARY KEY(postId, dialogueId));");

          // Video post-specific tables.
          statement
                  .execute("CREATE TABLE IF NOT EXISTS players(id INTEGER PRIMARY KEY AUTOINCREMENT, width TEXT NOT NULL, embedCode TEXT NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS videoPostPlayers(postId INTEGER NOT NULL REFERENCES videoPosts(id), playerId INTEGER NOT NULL REFERENCES players(id), playerIndex INTEGER NOT NULL, PRIMARY KEY(postId, playerId));");

          // Types table.
          statement
                  .execute("CREATE TABLE IF NOT EXISTS postTypes(id INTEGER PRIMARY KEY AUTOINCREMENT, type STRING UNIQUE NOT NULL);");
          try (PreparedStatement typeInsertStatement = connection
                  .prepareStatement("INSERT OR IGNORE INTO postTypes (type) VALUES (?)")) {
            for (PostType type : PostType.values()) {
              typeInsertStatement.setString(1, type.toString());
              typeInsertStatement.addBatch();
            }
            typeInsertStatement.executeBatch();
          }

          // Indexes.
          statement
                  .execute("CREATE INDEX IF NOT EXISTS postsPostTypeIdIndex ON posts(postTypeId);");
          statement.execute("CREATE INDEX IF NOT EXISTS postTagsPostIdIndex ON postTags(postId);");
          statement.execute("CREATE INDEX IF NOT EXISTS postTagsTagIdIndex ON postTags(tagId);");
          statement.execute("CREATE INDEX IF NOT EXISTS tagsTagIndex ON tags(tag);");
          statement
                  .execute("CREATE INDEX IF NOT EXISTS photoPostPhotosPostIdIndex ON photoPostPhotos(postId);");
          statement
                  .execute("CREATE INDEX IF NOT EXISTS photoPostPhotosPhotoIdIndex ON photoPostPhotos(photoId);");
          statement
                  .execute("CREATE INDEX IF NOT EXISTS photoPhotoSizesPhotoIdIndex ON photoPhotoSizes(photoId);");
          statement
                  .execute("CREATE INDEX IF NOT EXISTS photoPhotoSizesPhotoSizeIdIndex ON photoPhotoSizes(photoSizeId);");
          statement
                  .execute("CREATE INDEX IF NOT EXISTS chatPostDialoguePostIdIndex ON chatPostDialogue(postId);");
          statement
                  .execute("CREATE INDEX IF NOT EXISTS chatPostDialogueDialogueIdIndex ON chatPostDialogue(dialogueId);");
          statement
                  .execute("CREATE INDEX IF NOT EXISTS videoPostPlayersPostIdIndex ON videoPostPlayers(postId);");
          statement
                  .execute("CREATE INDEX IF NOT EXISTS videoPostPlayersPlayerIdIndex ON videoPostPlayers(playerId);");
          statement
                  .execute("CREATE UNIQUE INDEX IF NOT EXISTS postTypesTypeIndex ON postTypes(type);");

          return null;
        }
      }
    }.execute();
  }

  @Override
  public void put(final Post post) throws SQLException {
    new Transaction<Void, SQLException>() {

      @Override
      Void runTransaction() throws SQLException {
        doPut(post);
        return null;
      }
    }.execute();
  }

  private static String buildInQuery(int numItemsInSet) {
    Preconditions.checkArgument(numItemsInSet > 0);
    StringBuilder builder = new StringBuilder("?");
    while (--numItemsInSet > 0) {
      builder.append(", ?");
    }
    return builder.toString();
  }

  private static String buildTagSelectSql(int numTags) {
    StringBuilder builder = new StringBuilder("SELECT id, tag FROM tags WHERE tag IN (?");
    while (--numTags > 0) {
      builder.append(", ?");
    }
    builder.append(");");
    return builder.toString();
  }
}
