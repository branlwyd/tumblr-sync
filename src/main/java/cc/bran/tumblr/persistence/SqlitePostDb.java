package cc.bran.tumblr.persistence;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.joda.time.Instant;

import cc.bran.tumblr.types.Post;
import cc.bran.tumblr.types.PostType;
import cc.bran.tumblr.types.TextPost;

import com.google.common.annotations.VisibleForTesting;

/**
 * Persists {@link Post}s using an SQLite backend.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class SqlitePostDb implements PostDb {

  static {
    try {
      Class.forName("org.sqlite.JDBC");
    } catch (ClassNotFoundException exception) {
      throw new AssertionError("org.sqlite.JDBC must be available", exception);
    }
  }

  private final Connection connection;

  @VisibleForTesting
  SqlitePostDb(Connection connection) throws SQLException {
    this.connection = connection;
    initConnection(connection);
  }

  public SqlitePostDb(String dbFile) throws ClassNotFoundException, SQLException {
    this(DriverManager.getConnection(String.format("jdbc:sqlite:%s", new File(dbFile).getPath())));
  }

  @Override
  public Post get(long id) throws SQLException {
    try (PreparedStatement postRequestStatement = connection
            .prepareStatement("SELECT posts.blogName, posts.postUrl, posts.postedTimestamp, posts.retrievedTimestamp, posts.type, textPosts.title, textPosts.body FROM posts JOIN textPosts ON posts.id = textPosts.id WHERE posts.id = ?;");
            PreparedStatement tagsRequestStatement = connection
                    .prepareStatement("SELECT tags.tag FROM postTags JOIN tags ON postTags.tagId = tags.id WHERE postTags.postId = ?;")) {
      postRequestStatement.setLong(1, id);
      tagsRequestStatement.setLong(1, id);

      String blogName;
      String postUrl;
      Instant postedInstant;
      Instant retrievedInstant;
      Set<String> tags = new HashSet<String>();
      PostType type;
      String title;
      String body;

      // Get main data.
      try (ResultSet resultSet = postRequestStatement.executeQuery()) {
        if (!resultSet.next()) {
          connection.commit();
          return null;
        }

        blogName = resultSet.getString("blogName");
        postUrl = resultSet.getString("postUrl");
        postedInstant = new Instant(resultSet.getLong("postedTimestamp"));
        retrievedInstant = new Instant(resultSet.getLong("retrievedTimestamp"));
        type = PostType.valueOf(resultSet.getString("type"));
        title = resultSet.getString("title");
        body = resultSet.getString("body");
      }

      try (ResultSet resultSet = tagsRequestStatement.executeQuery()) {
        while (resultSet.next()) {
          tags.add(resultSet.getString("tag"));
        }
      }

      connection.commit();

      return new TextPost(id, blogName, postUrl, postedInstant, retrievedInstant, tags, title, body);
    } catch (SQLException exception) {
      connection.rollback();
      throw exception;
    }
  }

  @Override
  public void put(Post post) throws SQLException {
    try (PreparedStatement postsInsertStatement = connection
            .prepareStatement("INSERT OR REPLACE INTO posts (id, blogName, postUrl, postedTimestamp, retrievedTimestamp, type) VALUES (?, ?, ?, ?, ?, ?);");
            PreparedStatement textPostsInsertStatement = connection
                    .prepareStatement("INSERT OR REPLACE INTO textPosts (id, title, body) VALUES (?, ?, ?);");
            PreparedStatement postTagsDeleteStatement = connection
                    .prepareStatement("DELETE FROM postTags WHERE postId = ?;");
            PreparedStatement tagsSelectStatement = connection
                    .prepareStatement(buildTagSelectSql(post.getTags().size()));
            PreparedStatement tagsInsertStatement = connection
                    .prepareStatement("INSERT INTO tags (tag) VALUES (?);");
            PreparedStatement postTagsInsertStatement = connection
                    .prepareStatement("INSERT INTO postTags (postId, tagId) VALUES (?, ?);")) {
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
        Set<Integer> tagIds = new HashSet<>();
        Set<String> missingTags = new HashSet<>(textPost.getTags());
        try (ResultSet resultSet = tagsSelectStatement.executeQuery()) {
          while (resultSet.next()) {
            tagIds.add(resultSet.getInt("id"));
            missingTags.remove(resultSet.getString("tag"));
          }
        }

        // Create missing tags, if any.
        if (!missingTags.isEmpty()) {
          for (String tag : missingTags) {
            tagsInsertStatement.setString(1, tag);
            tagsInsertStatement.execute();
            try (ResultSet resultSet = tagsInsertStatement.getGeneratedKeys()) {
              tagIds.add(resultSet.getInt(1));
            }
          }
        }

        // Insert new entries into postTags table.
        if (!tagIds.isEmpty()) {
          for (Integer tagId : tagIds) {
            postTagsInsertStatement.setLong(1, textPost.getId());
            postTagsInsertStatement.setInt(2, tagId);
            postTagsInsertStatement.addBatch();
          }

          postTagsInsertStatement.executeBatch();
        }
      }

      connection.commit();
    } catch (SQLException exception) {
      connection.rollback();
      throw exception;
    }
  }

  private static String buildTagSelectSql(int numTags) {
    StringBuilder builder = new StringBuilder("SELECT id, tag FROM tags WHERE tag IN (?");
    while (--numTags > 0) {
      builder.append(", ?");
    }
    builder.append(");");
    return builder.toString();
  }

  private static void initConnection(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);

      statement.execute("PRAGMA foreign_keys = ON;");

      statement
              .execute("CREATE TABLE IF NOT EXISTS posts(id INTEGER PRIMARY KEY, blogName TEXT NOT NULL, postUrl TEXT NOT NULL, postedTimestamp INTEGER NOT NULL, retrievedTimestamp INTEGER NOT NULL, type TEXT NOT NULL REFERENCES postTypes(type));");
      statement
              .execute("CREATE TABLE IF NOT EXISTS textPosts(id INTEGER PRIMARY KEY REFERENCES posts(id), title TEXT NOT NULL, body TEXT NOT NULL);");

      statement
              .execute("CREATE TABLE IF NOT EXISTS tags(id INTEGER PRIMARY KEY ASC AUTOINCREMENT, tag TEXT UNIQUE NOT NULL);");
      statement
              .execute("CREATE TABLE IF NOT EXISTS postTags(postId INTEGER NOT NULL REFERENCES posts(id), tagId INTEGER NOT NULL REFERENCES tags(id), PRIMARY KEY(postId, tagId));");

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

      statement.execute("CREATE INDEX IF NOT EXISTS postsTypeIndex ON posts(type);");
      statement.execute("CREATE UNIQUE INDEX IF NOT EXISTS tagsTagIndex on tags(tag);");
      statement.execute("CREATE INDEX IF NOT EXISTS postTagsPostIdIndex ON postTags(postId);");
      statement.execute("CREATE INDEX IF NOT EXISTS postTagsTagIdIndex ON postTags(tagId);");
      statement.execute("CREATE UNIQUE INDEX IF NOT EXISTS postTypesTypeIndex ON postTypes(type);");

      connection.commit();
    }
  }
}
