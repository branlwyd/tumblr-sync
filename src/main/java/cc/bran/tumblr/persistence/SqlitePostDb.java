package cc.bran.tumblr.persistence;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.Instant;

import cc.bran.tumblr.types.AnswerPost;
import cc.bran.tumblr.types.AudioPost;
import cc.bran.tumblr.types.ChatPost;
import cc.bran.tumblr.types.ChatPost.Dialogue;
import cc.bran.tumblr.types.LinkPost;
import cc.bran.tumblr.types.PhotoPost;
import cc.bran.tumblr.types.PhotoPost.Photo;
import cc.bran.tumblr.types.PhotoPost.Photo.PhotoSize;
import cc.bran.tumblr.types.Post;
import cc.bran.tumblr.types.PostType;
import cc.bran.tumblr.types.QuotePost;
import cc.bran.tumblr.types.TextPost;
import cc.bran.tumblr.types.VideoPost;
import cc.bran.tumblr.types.VideoPost.Video;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Persists {@link Post}s using an SQLite backend.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class SqlitePostDb implements PostDb, AutoCloseable {

  private class ListQuery<T> implements AutoCloseable {

    private int idCount;

    private Iterator<List<T>> iterator;

    private PreparedStatement preparedStatement;

    private ResultSet resultSet;

    private final String sqlTemplate;

    public ListQuery(String sqlTemplate, Collection<T> ids) {
      this.sqlTemplate = sqlTemplate;
      this.iterator = Iterables.partition(ids, MAX_IDS_PER_QUERY).iterator();
      this.resultSet = null;
      this.preparedStatement = null;
      this.idCount = -1;
    }

    @Override
    public void close() throws SQLException {
      SQLException exception = null;

      iterator = null;

      if (preparedStatement != null) {
        try {
          preparedStatement.close();
        } catch (SQLException ex) {
          exception = ex;
        }
      }

      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException ex) {
          if (exception != null) {
            ex.addSuppressed(exception);
          }
          exception = ex;
        }
      }

      if (exception != null) {
        throw exception;
      }
    }

    public ResultSet getResultSet() {
      if (iterator == null) {
        throw new IllegalStateException("PostIdQuery is closed");
      }

      return resultSet;
    }

    public boolean next() throws SQLException {
      if (iterator == null) {
        throw new IllegalStateException("PostIdQuery is closed");
      }

      // Clean up previous result set.
      if (resultSet != null) {
        resultSet.close();
        resultSet = null;
      }

      // Check to see if there are any additional results.
      if (!iterator.hasNext()) {
        if (preparedStatement != null) {
          preparedStatement.close();
          preparedStatement = null;
        }
        return false;
      }

      // Set up the next query & return its result set.
      List<T> ids = iterator.next();
      if (idCount != ids.size()) {
        if (preparedStatement != null) {
          preparedStatement.close();
        }

        String sql = String.format(sqlTemplate, buildInQuery(ids.size()));
        preparedStatement = connection.prepareStatement(sql);
        idCount = ids.size();
      }

      int index = 1;
      for (T id : ids) {
        preparedStatement.setObject(index++, id);
      }
      resultSet = preparedStatement.executeQuery();
      return true;
    }
  }

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

  private static final String ANSWER_POST_INSERT_SQL = "INSERT INTO answerPosts (id, askingName, askingUrl, question, answer) VALUES (?, ?, ?, ?, ?);";

  private static final String ANSWER_POSTS_REQUEST_SQL_TEMPLATE = "SELECT id, askingName, askingUrl, question, answer FROM answerPosts WHERE id IN (%s);";

  private static final String AUDIO_POST_INSERT_SQL = "INSERT INTO audioPosts (id, album, albumArt, artist, caption, player, plays, trackName, trackNumber, year) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

  private static final String AUDIO_POSTS_REQUEST_SQL_TEMPLATE = "SELECT id, album, albumArt, artist, caption, player, plays, trackName, trackNumber, year FROM audioPosts WHERE id IN (%s);";

  private static final String CHAT_POST_DIALOGUE_INSERT_SQL = "INSERT INTO chatPostDialogue (postId, dialogueId, dialogueIndex) VALUES (?, ?, ?);";

  private static final String CHAT_POST_DIALOGUE_REQUEST_SQL_TEMPLATE = "SELECT chatPostDialogue.postId, dialogue.label, dialogue.name, dialogue.phrase FROM chatPostDialogue JOIN dialogue ON dialogue.id = chatPostDialogue.dialogueId WHERE chatPostDialogue.postId IN (%s) ORDER BY chatPostDialogue.dialogueIndex;";

  private static final String CHAT_POST_INSERT_SQL = "INSERT INTO chatPosts (id, body, title) VALUES (?, ?, ?);";

  private static final String CHAT_POSTS_REQUEST_SQL_TEMPLATE = "SELECT id, body, title FROM chatPosts WHERE id IN (%s);";

  private static final String DELETE_ANSWER_POSTS_SQL_TEMPLATE = "DELETE FROM answerPosts WHERE id IN (%s);";

  private static final String DELETE_AUDIO_POSTS_SQL_TEMPLATE = "DELETE FROM audioPosts WHERE id IN (%s);";

  private static final String DELETE_CHAT_POST_DIALOGUE_SQL_TEMPLATE = "DELETE FROM chatPostDialogue WHERE postId IN (%s);";

  private static final String DELETE_CHAT_POSTS_SQL_TEMPLATE = "DELETE FROM chatPosts WHERE id IN (%s);";

  private static final String DELETE_DIALOGUE_SQL_TEMPLATE = "DELETE FROM dialogue WHERE id IN (SELECT dialogueId FROM chatPostDialogue WHERE postId IN (%s));";

  private static final String DELETE_LINK_POSTS_SQL_TEMPLATE = "DELETE FROM linkPosts WHERE id IN (%s);";

  private static final String DELETE_PHOTO_PHOTO_SIZES_SQL_TEMPLATE = "DELETE FROM photoPhotoSizes WHERE photoId IN (SELECT photoId FROM photoPostPhotos WHERE postId IN (%s));";

  private static final String DELETE_PHOTO_POST_PHOTOS_SQL_TEMPLATE = "DELETE FROM photoPostPhotos WHERE postId IN (%s);";

  private static final String DELETE_PHOTO_POSTS_SQL_TEMPLATE = "DELETE FROM photoPosts WHERE id IN (%s);";

  private static final String DELETE_PHOTO_SIZES_SQL_TEMPLATE = "DELETE FROM photoSizes WHERE id IN (SELECT photoSizeId FROM photoPhotoSizes WHERE photoId IN (SELECT photoId FROM photoPostPhotos WHERE postId IN (%s)))";

  private static final String DELETE_PHOTOS_SQL_TEMPLATE = "DELETE FROM photos WHERE id IN (SELECT photoId FROM photoPostPhotos WHERE postId IN (%s));";

  private static final String DELETE_POST_TAGS_SQL_TEMPLATE = "DELETE FROM postTags WHERE postId IN (%s);";

  private static final String DELETE_POSTS_SQL_TEMPLATE = "DELETE FROM posts WHERE id IN (%s);";

  private static final String DELETE_QUOTE_POSTS_SQL_TEMPLATE = "DELETE FROM quotePosts WHERE id IN (%s);";

  private static final String DELETE_TEXT_POSTS_SQL_TEMPLATE = "DELETE FROM textPosts WHERE id IN (%s);";

  private static final String DELETE_VIDEO_POST_VIDEOS_SQL_TEMPLATE = "DELETE FROM videoPostVideos WHERE postId IN (%s);";

  private static final String DELETE_VIDEO_POSTS_SQL_TEMPLATE = "DELETE FROM videoPosts WHERE id IN (%s);";

  private static final String DELETE_VIDEOS_SQL_TEMPLATE = "DELETE FROM videos WHERE id IN (SELECT videoId FROM videoPostVideos WHERE postId IN (%s));";

  private static final String DIALOGUE_INSERT_SQL = "INSERT INTO dialogue (label, name, phrase) VALUES (?, ?, ?);";

  private static final String LINK_POST_INSERT_SQL = "INSERT INTO linkPosts (id, description, title, url) VALUES (?, ?, ?, ?);";

  private static final String LINK_POSTS_REQUEST_SQL_TEMPLATE = "SELECT id, description, title, url FROM linkPosts WHERE id IN (%s);";

  private static final int MAX_IDS_PER_QUERY = 999;

  private static final String PHOTO_INSERT_SQL = "INSERT INTO photos (caption) VALUES (?);";

  private static final String PHOTO_PHOTO_SIZE_INSERT_SQL = "INSERT INTO photoPhotoSizes (photoId, photoSizeId, photoSizeIndex) VALUES (?, ?, ?);";

  private static final String PHOTO_POST_INSERT_SQL = "INSERT INTO photoPosts (id, caption, height, width) VALUES (?, ?, ?, ?);";

  private static final String PHOTO_POST_PHOTO_INSERT_SQL = "INSERT INTO photoPostPhotos (postId, photoId, photoIndex) VALUES (?, ?, ?);";

  private static final String PHOTO_POSTS_REQUEST_SQL_TEMPLATE = "SELECT id, caption, height, width FROM photoPosts WHERE id IN (%s);";

  private static final String PHOTO_SIZE_INSERT_SQL = "INSERT INTO photoSizes (height, url, width) VALUES (?, ?, ?);";

  private static final String PHOTO_SIZES_REQUEST_SQL_TEMPLATE = "SELECT photoPostPhotos.photoId, photoSizes.height, photoSizes.url, photoSizes.width FROM photoPostPhotos JOIN photos ON photos.id = photoPostPhotos.photoId JOIN photoPhotoSizes ON photoPhotoSizes.photoId = photos.id JOIN photoSizes ON photoSizes.id = photoPhotoSizes.photoSizeId WHERE photoPostPhotos.postId IN (%s) ORDER BY photoPhotoSizes.photoSizeIndex;";

  private static final String PHOTOS_REQUEST_SQL_TEMPLATE = "SELECT photoPostPhotos.postId, photoPostPhotos.photoId, photos.caption FROM photoPostPhotos JOIN photos ON photos.id = photoPostPhotos.photoId WHERE photoPostPhotos.postId IN (%s) ORDER BY photoPostPhotos.photoIndex;";

  private static final String POST_INSERT_SQL = "INSERT INTO posts (id, blogName, postUrl, postedTimestamp, retrievedTimestamp, postTypeId) SELECT ?, ?, ?, ?, ?, id FROM postTypes WHERE type = ?;";

  private static final String POST_REQUEST_SQL = "SELECT posts.id, posts.blogName, posts.postUrl, posts.postedTimestamp, posts.retrievedTimestamp, postTypes.type FROM posts JOIN postTypes ON posts.postTypeId = postTypes.id WHERE posts.id = ?;";

  private static final String POST_TAG_INSERT_SQL = "INSERT INTO postTags (postId, tagId, tagIndex) VALUES (?, ?, ?);";

  private static final String POSTS_REQUEST_SQL = "SELECT posts.id, posts.blogName, posts.postUrl, posts.postedTimestamp, posts.retrievedTimestamp, postTypes.type FROM posts JOIN postTypes ON posts.postTypeId = postTypes.id;";

  private static final String QUOTE_POST_INSERT_SQL = "INSERT INTO quotePosts (id, source, text) VALUES (?, ?, ?);";

  private static final String QUOTE_POSTS_REQUEST_SQL_TEMPLATE = "SELECT id, source, text FROM quotePosts WHERE id IN (%s);";

  private static final String TAG_INSERT_SQL = "INSERT INTO tags (tag) VALUES (?);";

  private static final String TAG_REQUEST_BY_NAME_SQL_TEMPLATE = "SELECT id, tag FROM tags WHERE tag IN (%s);";

  private static final String TAGS_REQUEST_SQL_TEMPLATE = "SELECT postTags.postId, tags.tag FROM postTags JOIN tags ON postTags.tagId = tags.id WHERE postTags.postId IN (%s) ORDER BY postTags.tagIndex;";

  private static final String TEXT_POST_INSERT_SQL = "INSERT INTO textPosts (id, title, body) VALUES (?, ?, ?);";

  private static final String TEXT_POSTS_REQUEST_SQL_TEMPLATE = "SELECT id, title, body FROM textPosts WHERE id IN (%s);";

  private static final String VIDEO_INSERT_SQL = "INSERT INTO videos (embedCode, width) VALUES (?, ?);";

  private static final String VIDEO_POST_INSERT_SQL = "INSERT INTO videoPosts (id, caption) VALUES (?, ?);";

  private static final String VIDEO_POST_VIDEO_INSERT_SQL = "INSERT INTO videoPostVideos (postId, videoId, videoIndex) VALUES (?, ?, ?);";

  private static final String VIDEO_POST_VIDEOS_REQUEST_SQL_TEMPLATE = "SELECT videoPostVideos.postId, videos.embedCode, videos.width FROM videoPostVideos JOIN videos ON videos.id = videoPostVideos.videoId WHERE videoPostVideos.postId IN (%s) ORDER BY videoPostVideos.videoIndex;";

  private static final String VIDEO_POSTS_REQUEST_SQL_TEMPLATE = "SELECT id, caption FROM videoPosts WHERE id IN (%s);";

  static {
    try {
      Class.forName("org.sqlite.JDBC");
    } catch (ClassNotFoundException exception) {
      throw new AssertionError("org.sqlite.JDBC must be available", exception);
    }
  }

  private final PreparedStatement answerPostInsertStatement;

  private final PreparedStatement audioPostInsertStatement;

  private final PreparedStatement chatPostDialogueInsertStatement;

  private final PreparedStatement chatPostInsertStatement;

  private final Connection connection;

  private final PreparedStatement dialogueInsertStatement;

  private final PreparedStatement linkPostInsertStatement;

  private final PreparedStatement photoInsertStatement;

  private final PreparedStatement photoPhotoSizeInsertStatement;

  private final PreparedStatement photoPostInsertStatement;

  private final PreparedStatement photoPostPhotoInsertStatement;

  private final PreparedStatement photoSizeInsertStatement;

  private final PreparedStatement postInsertStatement;

  private final PreparedStatement postRequestStatement;

  private final PreparedStatement postsRequestStatement;

  private final PreparedStatement postTagInsertStatement;

  private final PreparedStatement quotePostInsertStatement;

  private final PreparedStatement tagInsertStatement;

  private final PreparedStatement textPostInsertStatement;

  private final PreparedStatement videoInsertStatement;

  private final PreparedStatement videoPostInsertStatement;

  private final PreparedStatement videoPostVideoInsertStatement;

  @VisibleForTesting
  SqlitePostDb(Connection connection) throws SQLException {
    this.connection = connection;
    initConnection();

    answerPostInsertStatement = connection.prepareStatement(ANSWER_POST_INSERT_SQL);
    audioPostInsertStatement = connection.prepareStatement(AUDIO_POST_INSERT_SQL);
    chatPostInsertStatement = connection.prepareStatement(CHAT_POST_INSERT_SQL);
    chatPostDialogueInsertStatement = connection.prepareStatement(CHAT_POST_DIALOGUE_INSERT_SQL);
    dialogueInsertStatement = connection.prepareStatement(DIALOGUE_INSERT_SQL);
    linkPostInsertStatement = connection.prepareStatement(LINK_POST_INSERT_SQL);
    photoInsertStatement = connection.prepareStatement(PHOTO_INSERT_SQL);
    photoPhotoSizeInsertStatement = connection.prepareStatement(PHOTO_PHOTO_SIZE_INSERT_SQL);
    photoPostInsertStatement = connection.prepareStatement(PHOTO_POST_INSERT_SQL);
    photoPostPhotoInsertStatement = connection.prepareStatement(PHOTO_POST_PHOTO_INSERT_SQL);
    photoSizeInsertStatement = connection.prepareStatement(PHOTO_SIZE_INSERT_SQL);
    postRequestStatement = connection.prepareStatement(POST_REQUEST_SQL);
    postsRequestStatement = connection.prepareStatement(POSTS_REQUEST_SQL);
    postInsertStatement = connection.prepareStatement(POST_INSERT_SQL);
    postTagInsertStatement = connection.prepareStatement(POST_TAG_INSERT_SQL);
    quotePostInsertStatement = connection.prepareStatement(QUOTE_POST_INSERT_SQL);
    tagInsertStatement = connection.prepareStatement(TAG_INSERT_SQL);
    textPostInsertStatement = connection.prepareStatement(TEXT_POST_INSERT_SQL);
    videoInsertStatement = connection.prepareStatement(VIDEO_INSERT_SQL);
    videoPostInsertStatement = connection.prepareStatement(VIDEO_POST_INSERT_SQL);
    videoPostVideoInsertStatement = connection.prepareStatement(VIDEO_POST_VIDEO_INSERT_SQL);
  }

  public SqlitePostDb(String dbFile) throws ClassNotFoundException, SQLException {
    this(DriverManager.getConnection(String.format("jdbc:sqlite:%s", new File(dbFile).getPath())));
  }

  @Override
  public void close() throws SQLException {
    answerPostInsertStatement.close();
    audioPostInsertStatement.close();
    chatPostInsertStatement.close();
    chatPostDialogueInsertStatement.close();
    dialogueInsertStatement.close();
    linkPostInsertStatement.close();
    photoInsertStatement.close();
    photoPhotoSizeInsertStatement.close();
    photoPostInsertStatement.close();
    photoPostPhotoInsertStatement.close();
    photoSizeInsertStatement.close();
    postRequestStatement.close();
    postsRequestStatement.close();
    postInsertStatement.close();
    postTagInsertStatement.close();
    quotePostInsertStatement.close();
    tagInsertStatement.close();
    textPostInsertStatement.close();
    videoInsertStatement.close();
    videoPostInsertStatement.close();
    videoPostVideoInsertStatement.close();
  }

  @Override
  public void delete(final long id) throws SQLException {
    new Transaction<Void, SQLException>() {

      @Override
      Void runTransaction() throws SQLException {
        doDelete(ImmutableList.of(id));
        return null;
      }
    }.execute();
  }

  private void doDelete(Collection<Long> ids) throws SQLException {
    // Delete answer post-related data.
    runDeleteQuery(ids, DELETE_ANSWER_POSTS_SQL_TEMPLATE);

    // Delete audio post-related data.
    runDeleteQuery(ids, DELETE_AUDIO_POSTS_SQL_TEMPLATE);

    // Delete chat post-related data.
    runDeleteQuery(ids, DELETE_DIALOGUE_SQL_TEMPLATE);
    runDeleteQuery(ids, DELETE_CHAT_POST_DIALOGUE_SQL_TEMPLATE);
    runDeleteQuery(ids, DELETE_CHAT_POSTS_SQL_TEMPLATE);

    // Delete link post-related data.
    runDeleteQuery(ids, DELETE_LINK_POSTS_SQL_TEMPLATE);

    // Delete photo post-related data.
    runDeleteQuery(ids, DELETE_PHOTO_SIZES_SQL_TEMPLATE);
    runDeleteQuery(ids, DELETE_PHOTO_PHOTO_SIZES_SQL_TEMPLATE);
    runDeleteQuery(ids, DELETE_PHOTOS_SQL_TEMPLATE);
    runDeleteQuery(ids, DELETE_PHOTO_POST_PHOTOS_SQL_TEMPLATE);
    runDeleteQuery(ids, DELETE_PHOTO_POSTS_SQL_TEMPLATE);

    // Delete quote post-related data.
    runDeleteQuery(ids, DELETE_QUOTE_POSTS_SQL_TEMPLATE);

    // Delete text post-related data.
    runDeleteQuery(ids, DELETE_TEXT_POSTS_SQL_TEMPLATE);

    // Delete video post-related data.
    runDeleteQuery(ids, DELETE_VIDEOS_SQL_TEMPLATE);
    runDeleteQuery(ids, DELETE_VIDEO_POST_VIDEOS_SQL_TEMPLATE);
    runDeleteQuery(ids, DELETE_VIDEO_POSTS_SQL_TEMPLATE);

    // Delete tag-related data.
    runDeleteQuery(ids, DELETE_POST_TAGS_SQL_TEMPLATE);

    // Delete post-related data.
    runDeleteQuery(ids, DELETE_POSTS_SQL_TEMPLATE);
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

  private List<Post> doGetAll() throws SQLException {
    try (ResultSet resultSet = postsRequestStatement.executeQuery()) {
      return doGetFromResultSet(resultSet);
    }
  }

  private void doGetAnswerPostData(Map<Long, AnswerPost.Builder> builderById) throws SQLException {
    if (builderById.isEmpty()) {
      return;
    }

    try (ListQuery<Long> answerPostsQuery = new ListQuery<Long>(ANSWER_POSTS_REQUEST_SQL_TEMPLATE,
            builderById.keySet())) {
      while (answerPostsQuery.next()) {
        ResultSet resultSet = answerPostsQuery.getResultSet();
        while (resultSet.next()) {
          AnswerPost.Builder postBuilder = builderById.get(resultSet.getLong("id"));
          postBuilder.setAskingName(resultSet.getString("askingName"));
          postBuilder.setAskingUrl(resultSet.getString("askingUrl"));
          postBuilder.setQuestion(resultSet.getString("question"));
          postBuilder.setAnswer(resultSet.getString("answer"));
        }
      }
    }
  }

  private void doGetAudioPostData(Map<Long, AudioPost.Builder> builderById) throws SQLException {
    if (builderById.isEmpty()) {
      return;
    }

    try (ListQuery<Long> audioPostsQuery = new ListQuery<Long>(AUDIO_POSTS_REQUEST_SQL_TEMPLATE,
            builderById.keySet())) {
      while (audioPostsQuery.next()) {
        ResultSet resultSet = audioPostsQuery.getResultSet();
        while (resultSet.next()) {
          AudioPost.Builder postBuilder = builderById.get(resultSet.getLong("id"));
          postBuilder.setAlbum(resultSet.getString("album"));
          postBuilder.setAlbumArt(resultSet.getString("albumArt"));
          postBuilder.setArtist(resultSet.getString("artist"));
          postBuilder.setCaption(resultSet.getString("caption"));
          postBuilder.setPlayer(resultSet.getString("player"));
          postBuilder.setPlays(resultSet.getInt("plays"));
          postBuilder.setTrackName(resultSet.getString("trackName"));
          postBuilder.setTrackNumber(resultSet.getInt("trackNumber"));
          postBuilder.setYear(resultSet.getInt("year"));
        }
      }
    }
  }

  private void doGetChatPostData(Map<Long, ChatPost.Builder> builderById) throws SQLException {
    if (builderById.isEmpty()) {
      return;
    }

    // Get basic chat post data.
    try (ListQuery<Long> chatPostsQuery = new ListQuery<Long>(CHAT_POSTS_REQUEST_SQL_TEMPLATE,
            builderById.keySet())) {
      while (chatPostsQuery.next()) {
        ResultSet resultSet = chatPostsQuery.getResultSet();
        while (resultSet.next()) {
          ChatPost.Builder postBuilder = builderById.get(resultSet.getLong("id"));
          postBuilder.setBody(resultSet.getString("body"));
          postBuilder.setTitle(resultSet.getString("title"));
        }
      }
    }

    // Get dialogue.
    Map<Long, ImmutableList.Builder<Dialogue>> dialogueBuilderById = new HashMap<>();
    for (long id : builderById.keySet()) {
      dialogueBuilderById.put(id, new ImmutableList.Builder<Dialogue>());
    }

    try (ListQuery<Long> chatPostDialogueQuery = new ListQuery<Long>(
            CHAT_POST_DIALOGUE_REQUEST_SQL_TEMPLATE, builderById.keySet())) {
      while (chatPostDialogueQuery.next()) {
        ResultSet resultSet = chatPostDialogueQuery.getResultSet();
        while (resultSet.next()) {
          ImmutableList.Builder<Dialogue> dialogueBuilder = dialogueBuilderById.get(resultSet
                  .getLong("postId"));
          dialogueBuilder.add(new Dialogue(resultSet.getString("name"), resultSet
                  .getString("label"), resultSet.getString("phrase")));
        }
      }
    }

    for (Map.Entry<Long, ChatPost.Builder> entry : builderById.entrySet()) {
      long id = entry.getKey();
      ChatPost.Builder postBuilder = entry.getValue();

      postBuilder.setDialogue(dialogueBuilderById.get(id).build());
    }
  }

  private List<Post> doGetFromResultSet(ResultSet resultSet) throws SQLException {
    Map<Long, Post.Builder> builderById = new HashMap<>();
    Map<Long, AnswerPost.Builder> answerBuilderById = new HashMap<>();
    Map<Long, AudioPost.Builder> audioBuilderById = new HashMap<>();
    Map<Long, ChatPost.Builder> chatBuilderById = new HashMap<>();
    Map<Long, LinkPost.Builder> linkBuilderById = new HashMap<>();
    Map<Long, PhotoPost.Builder> photoBuilderById = new HashMap<>();
    Map<Long, QuotePost.Builder> quoteBuilderById = new HashMap<>();
    Map<Long, TextPost.Builder> textBuilderById = new HashMap<>();
    Map<Long, VideoPost.Builder> videoBuilderById = new HashMap<>();

    // Extract basic data from results & categorize them by type.
    while (resultSet.next()) {
      Post.Builder postBuilder;
      long id = resultSet.getLong("id");
      PostType postType = PostType.valueOf(resultSet.getString("type"));

      switch (postType) {
      case ANSWER:
        postBuilder = new AnswerPost.Builder();
        answerBuilderById.put(id, (AnswerPost.Builder) postBuilder);
        break;
      case AUDIO:
        postBuilder = new AudioPost.Builder();
        audioBuilderById.put(id, (AudioPost.Builder) postBuilder);
        break;
      case CHAT:
        postBuilder = new ChatPost.Builder();
        chatBuilderById.put(id, (ChatPost.Builder) postBuilder);
        break;
      case LINK:
        postBuilder = new LinkPost.Builder();
        linkBuilderById.put(id, (LinkPost.Builder) postBuilder);
        break;
      case PHOTO:
        postBuilder = new PhotoPost.Builder();
        photoBuilderById.put(id, (PhotoPost.Builder) postBuilder);
        break;
      case QUOTE:
        postBuilder = new QuotePost.Builder();
        quoteBuilderById.put(id, (QuotePost.Builder) postBuilder);
        break;
      case TEXT:
        postBuilder = new TextPost.Builder();
        textBuilderById.put(id, (TextPost.Builder) postBuilder);
        break;
      case VIDEO:
        postBuilder = new VideoPost.Builder();
        videoBuilderById.put(id, (VideoPost.Builder) postBuilder);
        break;
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
    doGetAnswerPostData(answerBuilderById);
    doGetAudioPostData(audioBuilderById);
    doGetChatPostData(chatBuilderById);
    doGetLinkPostData(linkBuilderById);
    doGetPhotoPostData(photoBuilderById);
    doGetQuotePostData(quoteBuilderById);
    doGetTextPostData(textBuilderById);
    doGetVideoPostData(videoBuilderById);

    // Build result.
    ImmutableList.Builder<Post> resultBuilder = ImmutableList.builder();
    for (Post.Builder postBuilder : builderById.values()) {
      resultBuilder.add(postBuilder.build());
    }
    return resultBuilder.build();
  }

  private void doGetLinkPostData(Map<Long, LinkPost.Builder> builderById) throws SQLException {
    if (builderById.isEmpty()) {
      return;
    }

    try (ListQuery<Long> linkPostsQuery = new ListQuery<Long>(LINK_POSTS_REQUEST_SQL_TEMPLATE,
            builderById.keySet())) {
      while (linkPostsQuery.next()) {
        ResultSet resultSet = linkPostsQuery.getResultSet();
        while (resultSet.next()) {
          LinkPost.Builder postBuilder = builderById.get(resultSet.getLong("id"));
          postBuilder.setDescription(resultSet.getString("description"));
          postBuilder.setTitle(resultSet.getString("title"));
          postBuilder.setUrl(resultSet.getString("url"));
        }
      }
    }
  }

  private void doGetPhotoPostData(Map<Long, PhotoPost.Builder> builderById) throws SQLException {
    if (builderById.isEmpty()) {
      return;
    }

    // Get photo sizes data.
    Map<Integer, ImmutableList.Builder<PhotoSize>> photoSizesByPhotoId = new HashMap<>();

    try (ListQuery<Long> photoSizesQuery = new ListQuery<Long>(PHOTO_SIZES_REQUEST_SQL_TEMPLATE,
            builderById.keySet())) {
      while (photoSizesQuery.next()) {
        ResultSet resultSet = photoSizesQuery.getResultSet();
        while (resultSet.next()) {
          int photoId = resultSet.getInt("photoId");
          PhotoSize photoSize = new PhotoSize(resultSet.getInt("width"),
                  resultSet.getInt("height"), resultSet.getString("url"));

          if (!photoSizesByPhotoId.containsKey(photoId)) {
            photoSizesByPhotoId.put(photoId, new ImmutableList.Builder<PhotoSize>());
          }

          photoSizesByPhotoId.get(photoId).add(photoSize);
        }
      }
    }

    // Get photos data.
    Map<Long, ImmutableList.Builder<Photo>> photosByPostId = new HashMap<>();
    for (long id : builderById.keySet()) {
      photosByPostId.put(id, new ImmutableList.Builder<Photo>());
    }

    try (ListQuery<Long> photosQuery = new ListQuery<Long>(PHOTOS_REQUEST_SQL_TEMPLATE,
            builderById.keySet())) {
      while (photosQuery.next()) {
        ResultSet resultSet = photosQuery.getResultSet();
        while (resultSet.next()) {
          long postId = resultSet.getLong("postId");
          int photoId = resultSet.getInt("photoId");

          List<PhotoSize> photoSizes;
          if (photoSizesByPhotoId.containsKey(photoId)) {
            photoSizes = photoSizesByPhotoId.get(photoId).build();
          } else {
            photoSizes = ImmutableList.of();
          }

          Photo photo = new Photo(resultSet.getString("caption"), photoSizes);
          photosByPostId.get(postId).add(photo);
        }
      }
    }

    // Get photo post data.
    try (ListQuery<Long> photoPostsQuery = new ListQuery<Long>(PHOTO_POSTS_REQUEST_SQL_TEMPLATE,
            builderById.keySet())) {
      while (photoPostsQuery.next()) {
        ResultSet resultSet = photoPostsQuery.getResultSet();
        while (resultSet.next()) {
          long postId = resultSet.getLong("id");
          PhotoPost.Builder builder = builderById.get(postId);
          builder.setCaption(resultSet.getString("caption"));
          builder.setPhotos(photosByPostId.get(postId).build());

          int height = resultSet.getInt("height");
          if (!resultSet.wasNull()) {
            builder.setHeight(height);
          }

          int width = resultSet.getInt("width");
          if (!resultSet.wasNull()) {
            builder.setWidth(width);
          }
        }
      }
    }
  }

  private void doGetQuotePostData(Map<Long, QuotePost.Builder> builderById) throws SQLException {
    if (builderById.isEmpty()) {
      return;
    }

    try (ListQuery<Long> quotePostsQuery = new ListQuery<Long>(QUOTE_POSTS_REQUEST_SQL_TEMPLATE,
            builderById.keySet())) {
      while (quotePostsQuery.next()) {
        ResultSet resultSet = quotePostsQuery.getResultSet();
        while (resultSet.next()) {
          QuotePost.Builder builder = builderById.get(resultSet.getLong("id"));
          builder.setSource(resultSet.getString("source"));
          builder.setText(resultSet.getString("text"));
        }
      }
    }
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
    try (ListQuery<Long> tagsRequestQuery = new ListQuery<Long>(TAGS_REQUEST_SQL_TEMPLATE,
            builderById.keySet())) {
      while (tagsRequestQuery.next()) {
        ResultSet resultSet = tagsRequestQuery.getResultSet();

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

    try (ListQuery<Long> textPostsQuery = new ListQuery<Long>(TEXT_POSTS_REQUEST_SQL_TEMPLATE,
            builderById.keySet())) {
      while (textPostsQuery.next()) {
        ResultSet resultSet = textPostsQuery.getResultSet();
        while (resultSet.next()) {
          TextPost.Builder builder = builderById.get(resultSet.getLong("id"));
          builder.setTitle(resultSet.getString("title"));
          builder.setBody(resultSet.getString("body"));
        }
      }
    }
  }

  private void doGetVideoPostData(Map<Long, VideoPost.Builder> builderById) throws SQLException {
    if (builderById.isEmpty()) {
      return;
    }

    // Get basic video post information.
    try (ListQuery<Long> videoPostsQuery = new ListQuery<Long>(VIDEO_POSTS_REQUEST_SQL_TEMPLATE,
            builderById.keySet())) {
      while (videoPostsQuery.next()) {
        ResultSet resultSet = videoPostsQuery.getResultSet();
        while (resultSet.next()) {
          VideoPost.Builder postBuilder = builderById.get(resultSet.getLong("id"));
          postBuilder.setCaption(resultSet.getString("caption"));
        }
      }
    }

    // Get videos.
    Map<Long, ImmutableList.Builder<Video>> videoBuilderById = new HashMap<>();
    for (long id : builderById.keySet()) {
      videoBuilderById.put(id, new ImmutableList.Builder<Video>());
    }

    try (ListQuery<Long> videoPostVideosQuery = new ListQuery<Long>(
            VIDEO_POST_VIDEOS_REQUEST_SQL_TEMPLATE, builderById.keySet())) {
      while (videoPostVideosQuery.next()) {
        ResultSet resultSet = videoPostVideosQuery.getResultSet();
        while (resultSet.next()) {
          ImmutableList.Builder<Video> videoBuilder = videoBuilderById.get(resultSet
                  .getLong("postId"));
          videoBuilder.add(new Video(resultSet.getInt("width"), resultSet.getString("embedCode")));
        }
      }
    }

    for (long id : builderById.keySet()) {
      builderById.get(id).setPlayers(videoBuilderById.get(id).build());
    }
  }

  private void doPut(Collection<Post> posts) throws SQLException {
    if (posts.isEmpty()) {
      return;
    }

    // Categorize post by type & update basic post information.
    Map<Long, Post> postById = new HashMap<>();
    Map<Long, AnswerPost> answerPostById = new HashMap<>();
    Map<Long, AudioPost> audioPostById = new HashMap<>();
    Map<Long, ChatPost> chatPostById = new HashMap<>();
    Map<Long, LinkPost> linkPostById = new HashMap<>();
    Map<Long, PhotoPost> photoPostById = new HashMap<>();
    Map<Long, QuotePost> quotePostById = new HashMap<>();
    Map<Long, TextPost> textPostById = new HashMap<>();
    Map<Long, VideoPost> videoPostById = new HashMap<>();

    for (Post post : posts) {
      postById.put(post.getId(), post);

      switch (post.getType()) {
      case ANSWER:
        answerPostById.put(post.getId(), (AnswerPost) post);
        break;
      case AUDIO:
        audioPostById.put(post.getId(), (AudioPost) post);
        break;
      case CHAT:
        chatPostById.put(post.getId(), (ChatPost) post);
        break;
      case LINK:
        linkPostById.put(post.getId(), (LinkPost) post);
        break;
      case PHOTO:
        photoPostById.put(post.getId(), (PhotoPost) post);
        break;
      case QUOTE:
        quotePostById.put(post.getId(), (QuotePost) post);
        break;
      case TEXT:
        textPostById.put(post.getId(), (TextPost) post);
        break;
      case VIDEO:
        videoPostById.put(post.getId(), (VideoPost) post);
        break;
      default:
        throw new AssertionError(String.format("Post %d has impossible type %s.", post.getId(),
                post.getType().toString()));
      }
    }

    // Delete existing post information.
    doDelete(postById.keySet());

    // Update basic post information.
    for (Post post : posts) {
      postInsertStatement.setLong(1, post.getId());
      postInsertStatement.setString(2, post.getBlogName());
      postInsertStatement.setString(3, post.getPostUrl());
      postInsertStatement.setLong(4, post.getPostedInstant().getMillis());
      postInsertStatement.setLong(5, post.getRetrievedInstant().getMillis());
      postInsertStatement.setString(6, post.getType().toString());
      postInsertStatement.addBatch();
    }
    postInsertStatement.executeBatch();

    // Update tag & post-type specific information.
    doPutTagData(postById);
    doPutAudioPostData(audioPostById);
    doPutAnswerPostData(answerPostById);
    doPutChatPostData(chatPostById);
    doPutLinkPostData(linkPostById);
    doPutPhotoPostData(photoPostById);
    doPutQuotePostData(quotePostById);
    doPutTextPostData(textPostById);
    doPutVideoPostData(videoPostById);
  }

  private void doPutAnswerPostData(Map<Long, AnswerPost> postById) throws SQLException {
    if (postById.isEmpty()) {
      return;
    }

    for (AnswerPost post : postById.values()) {
      answerPostInsertStatement.setLong(1, post.getId());
      answerPostInsertStatement.setString(2, post.getAskingName());
      answerPostInsertStatement.setString(3, post.getAskingUrl());
      answerPostInsertStatement.setString(4, post.getQuestion());
      answerPostInsertStatement.setString(5, post.getAnswer());
      answerPostInsertStatement.addBatch();
    }
    answerPostInsertStatement.executeBatch();
  }

  private void doPutAudioPostData(Map<Long, AudioPost> postById) throws SQLException {
    if (postById.isEmpty()) {
      return;
    }

    for (AudioPost post : postById.values()) {
      audioPostInsertStatement.setLong(1, post.getId());
      audioPostInsertStatement.setString(2, post.getAlbum());
      audioPostInsertStatement.setString(3, post.getAlbumArt());
      audioPostInsertStatement.setString(4, post.getArtist());
      audioPostInsertStatement.setString(5, post.getCaption());
      audioPostInsertStatement.setString(6, post.getPlayer());
      audioPostInsertStatement.setInt(7, post.getPlays());
      audioPostInsertStatement.setString(8, post.getTrackName());
      audioPostInsertStatement.setInt(9, post.getTrackNumber());
      audioPostInsertStatement.setInt(10, post.getYear());
      audioPostInsertStatement.addBatch();
    }
    audioPostInsertStatement.executeBatch();
  }

  private void doPutChatPostData(Map<Long, ChatPost> postById) throws SQLException {
    if (postById.isEmpty()) {
      return;
    }

    // Insert dialogue.
    int totalDialogue = 0;
    Map<Long, List<Integer>> dialogueIdsByPostId = new HashMap<>();
    for (ChatPost post : postById.values()) {
      List<Integer> dialogueIds = new ArrayList<Integer>();
      for (Dialogue dialogue : post.getDialogue()) {
        totalDialogue++;

        dialogueInsertStatement.setString(1, dialogue.getLabel());
        dialogueInsertStatement.setString(2, dialogue.getName());
        dialogueInsertStatement.setString(3, dialogue.getPhrase());
        dialogueInsertStatement.execute();

        try (ResultSet resultSet = dialogueInsertStatement.getGeneratedKeys()) {
          int id = resultSet.getInt(1);
          dialogueIds.add(id);
        }
      }
      dialogueIdsByPostId.put(post.getId(), dialogueIds);
    }

    // Insert chatPosts.
    for (ChatPost post : postById.values()) {
      chatPostInsertStatement.setLong(1, post.getId());
      chatPostInsertStatement.setString(2, post.getBody());
      chatPostInsertStatement.setString(3, post.getTitle());
      chatPostInsertStatement.addBatch();
    }
    chatPostInsertStatement.executeBatch();

    // Insert chatPostDialogue.
    if (totalDialogue == 0) {
      return;
    }

    for (long postId : postById.keySet()) {
      List<Integer> dialogueIds = dialogueIdsByPostId.get(postId);

      int index = 0;
      for (int dialogueId : dialogueIds) {
        chatPostDialogueInsertStatement.setLong(1, postId);
        chatPostDialogueInsertStatement.setInt(2, dialogueId);
        chatPostDialogueInsertStatement.setInt(3, index++);
        chatPostDialogueInsertStatement.addBatch();
      }
    }
    chatPostDialogueInsertStatement.executeBatch();
  }

  private void doPutLinkPostData(Map<Long, LinkPost> postById) throws SQLException {
    if (postById.isEmpty()) {
      return;
    }

    for (LinkPost post : postById.values()) {
      linkPostInsertStatement.setLong(1, post.getId());
      linkPostInsertStatement.setString(2, post.getDescription());
      linkPostInsertStatement.setString(3, post.getTitle());
      linkPostInsertStatement.setString(4, post.getUrl());
      linkPostInsertStatement.addBatch();
    }
    linkPostInsertStatement.executeBatch();
  }

  private void doPutPhotoPostData(Map<Long, PhotoPost> postById) throws SQLException {
    if (postById.isEmpty()) {
      return;
    }

    for (PhotoPost post : postById.values()) {
      photoPostInsertStatement.setLong(1, post.getId());
      photoPostInsertStatement.setString(2, post.getCaption());
      if (post.getHeight().isPresent()) {
        photoPostInsertStatement.setInt(3, post.getHeight().get());
      }
      if (post.getWidth().isPresent()) {
        photoPostInsertStatement.setInt(4, post.getWidth().get());
      }
      photoPostInsertStatement.addBatch();

      // Add photos.
      int photoIndex = 0;
      for (Photo photo : post.getPhotos()) {
        int photoId;
        photoInsertStatement.setString(1, photo.getCaption());
        photoInsertStatement.execute();

        try (ResultSet resultSet = photoInsertStatement.getGeneratedKeys()) {
          photoId = resultSet.getInt(1);
        }

        photoPostPhotoInsertStatement.setLong(1, post.getId());
        photoPostPhotoInsertStatement.setInt(2, photoId);
        photoPostPhotoInsertStatement.setInt(3, photoIndex++);
        photoPostPhotoInsertStatement.addBatch();

        int photoSizeIndex = 0;
        for (PhotoSize photoSize : photo.getPhotoSizes()) {
          int photoSizeId;
          photoSizeInsertStatement.setInt(1, photoSize.getHeight());
          photoSizeInsertStatement.setString(2, photoSize.getUrl());
          photoSizeInsertStatement.setInt(3, photoSize.getWidth());
          photoSizeInsertStatement.execute();

          try (ResultSet resultSet = photoSizeInsertStatement.getGeneratedKeys()) {
            photoSizeId = resultSet.getInt(1);
          }

          photoPhotoSizeInsertStatement.setInt(1, photoId);
          photoPhotoSizeInsertStatement.setInt(2, photoSizeId);
          photoPhotoSizeInsertStatement.setInt(3, photoSizeIndex++);
          photoPhotoSizeInsertStatement.addBatch();
        }
      }
    }
    photoPostInsertStatement.executeBatch();
    photoPostPhotoInsertStatement.executeBatch();
    photoPhotoSizeInsertStatement.executeBatch();
  }

  private void doPutQuotePostData(Map<Long, QuotePost> postById) throws SQLException {
    if (postById.isEmpty()) {
      return;
    }

    for (QuotePost post : postById.values()) {
      quotePostInsertStatement.setLong(1, post.getId());
      quotePostInsertStatement.setString(2, post.getSource());
      quotePostInsertStatement.setString(3, post.getText());
      quotePostInsertStatement.addBatch();
    }
    quotePostInsertStatement.executeBatch();
  }

  private void doPutTagData(Map<Long, Post> postById) throws SQLException {
    if (postById.isEmpty()) {
      return;
    }

    // Find IDs for existing tags.
    Map<String, Integer> idByTag = new HashMap<>();
    for (Post post : postById.values()) {
      for (String tag : post.getTags()) {
        idByTag.put(tag, null);
      }
    }

    if (idByTag.isEmpty()) {
      return;
    }

    try (ListQuery<String> tagRequestByNamesQuery = new ListQuery<String>(
            TAG_REQUEST_BY_NAME_SQL_TEMPLATE, idByTag.keySet())) {
      while (tagRequestByNamesQuery.next()) {
        ResultSet resultSet = tagRequestByNamesQuery.getResultSet();
        while (resultSet.next()) {
          int id = resultSet.getInt("id");
          String tag = resultSet.getString("tag");

          idByTag.put(tag, id);
        }
      }
    }

    // Create missing tags, if any.
    for (Map.Entry<String, Integer> entry : idByTag.entrySet()) {
      if (entry.getValue() != null) {
        continue;
      }

      String tag = entry.getKey();
      tagInsertStatement.setString(1, tag);
      tagInsertStatement.execute();
      try (ResultSet resultSet = tagInsertStatement.getGeneratedKeys()) {
        int id = resultSet.getInt(1);
        entry.setValue(id);
      }
    }

    // Update the postTags table.
    for (Post post : postById.values()) {
      int index = 0;
      for (String tag : post.getTags()) {
        postTagInsertStatement.setLong(1, post.getId());
        postTagInsertStatement.setInt(2, idByTag.get(tag));
        postTagInsertStatement.setInt(3, index++);
        postTagInsertStatement.addBatch();
      }
    }
    postTagInsertStatement.executeBatch();
  }

  private void doPutTextPostData(Map<Long, TextPost> postById) throws SQLException {
    if (postById.isEmpty()) {
      return;
    }

    for (TextPost post : postById.values()) {
      textPostInsertStatement.setLong(1, post.getId());
      textPostInsertStatement.setString(2, post.getTitle());
      textPostInsertStatement.setString(3, post.getBody());
      textPostInsertStatement.addBatch();
    }
    textPostInsertStatement.executeBatch();
  }

  private void doPutVideoPostData(Map<Long, VideoPost> postById) throws SQLException {
    if (postById.isEmpty()) {
      return;
    }

    // Put basic video post data.
    for (VideoPost post : postById.values()) {
      videoPostInsertStatement.setLong(1, post.getId());
      videoPostInsertStatement.setString(2, post.getCaption());
      videoPostInsertStatement.addBatch();
    }
    videoPostInsertStatement.executeBatch();

    // Put videos.
    int totalVideos = 0;
    Map<Long, List<Integer>> videoIdsByPostId = new HashMap<>();
    for (VideoPost post : postById.values()) {
      List<Integer> videoIds = new ArrayList<>();

      for (Video video : post.getPlayers()) {
        totalVideos++;
        videoInsertStatement.setString(1, video.getEmbedCode());
        videoInsertStatement.setInt(2, video.getWidth());
        videoInsertStatement.execute();

        try (ResultSet resultSet = videoInsertStatement.getGeneratedKeys()) {
          int id = resultSet.getInt(1);
          videoIds.add(id);
        }
      }

      videoIdsByPostId.put(post.getId(), videoIds);
    }

    // Put videoPostVideos.
    if (totalVideos == 0) {
      return;
    }

    for (Map.Entry<Long, List<Integer>> entry : videoIdsByPostId.entrySet()) {
      long postId = entry.getKey();
      List<Integer> videoIds = entry.getValue();

      int index = 0;
      for (int videoId : videoIds) {
        videoPostVideoInsertStatement.setLong(1, postId);
        videoPostVideoInsertStatement.setInt(2, videoId);
        videoPostVideoInsertStatement.setInt(3, index++);
        videoPostVideoInsertStatement.addBatch();
      }
    }
    videoPostVideoInsertStatement.executeBatch();
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

  @Override
  public List<Post> getAll() throws SQLException {
    return new Transaction<List<Post>, SQLException>() {

      @Override
      List<Post> runTransaction() throws SQLException {
        return doGetAll();
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
                  .execute("CREATE TABLE IF NOT EXISTS photoPosts(id INTEGER PRIMARY KEY REFERENCES posts(id), caption TEXT NOT NULL, width INTEGER, height INTEGER);");
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
                  .execute("CREATE TABLE IF NOT EXISTS videos(id INTEGER PRIMARY KEY AUTOINCREMENT, width TEXT NOT NULL, embedCode TEXT NOT NULL);");
          statement
                  .execute("CREATE TABLE IF NOT EXISTS videoPostVideos(postId INTEGER NOT NULL REFERENCES videoPosts(id), videoId INTEGER NOT NULL REFERENCES videos(id), videoIndex INTEGER NOT NULL, PRIMARY KEY(postId, videoId));");

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
                  .execute("CREATE INDEX IF NOT EXISTS videoPostVideosPostIdIndex ON videoPostVideos(postId);");
          statement
                  .execute("CREATE INDEX IF NOT EXISTS videoPostVideosVideoIdIndex ON videoPostVideos(videoId);");
          statement
                  .execute("CREATE UNIQUE INDEX IF NOT EXISTS postTypesTypeIndex ON postTypes(type);");

          return null;
        }
      }
    }.execute();
  }

  @Override
  public void put(final Collection<Post> posts) throws SQLException {
    new Transaction<Void, SQLException>() {

      @Override
      Void runTransaction() throws SQLException {
        doPut(posts);
        return null;
      }
    }.execute();
  }

  @Override
  public void put(final Post post) throws SQLException {
    new Transaction<Void, SQLException>() {

      @Override
      Void runTransaction() throws SQLException {
        doPut(ImmutableList.of(post));
        return null;
      }
    }.execute();
  }

  private void runDeleteQuery(Collection<Long> ids, String sqlTemplate) throws SQLException {
    for (List<Long> partitionedIds : Iterables.partition(ids, MAX_IDS_PER_QUERY)) {
      String sql = String.format(sqlTemplate, buildInQuery(partitionedIds.size()));
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        int index = 1;
        for (long id : partitionedIds) {
          statement.setLong(index++, id);
        }
        statement.execute();
      }
    }
  }

  private static String buildInQuery(int numItemsInSet) {
    Preconditions.checkArgument(numItemsInSet > 0);
    StringBuilder builder = new StringBuilder("?");
    while (--numItemsInSet > 0) {
      builder.append(", ?");
    }
    return builder.toString();
  }
}
