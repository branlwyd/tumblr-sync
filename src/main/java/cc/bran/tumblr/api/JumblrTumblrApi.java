package cc.bran.tumblr.api;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.joda.time.Instant;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import cc.bran.tumblr.types.AnswerPost;
import cc.bran.tumblr.types.AudioPost;
import cc.bran.tumblr.types.ChatPost;
import cc.bran.tumblr.types.ChatPost.Dialogue;
import cc.bran.tumblr.types.LinkPost;
import cc.bran.tumblr.types.PhotoPost;
import cc.bran.tumblr.types.PhotoPost.Photo;
import cc.bran.tumblr.types.PhotoPost.Photo.PhotoSize;
import cc.bran.tumblr.types.Post;
import cc.bran.tumblr.types.QuotePost;
import cc.bran.tumblr.types.TextPost;
import cc.bran.tumblr.types.VideoPost;
import cc.bran.tumblr.types.VideoPost.Video;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tumblr.jumblr.JumblrClient;

/**
 * A {@link TumblrApi} implementation backed by the Jumblr API.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class JumblrTumblrApi implements TumblrApi {

  private class AllPostsIterator implements Iterator<Post> {

    private final String blogName;

    private int offset;

    private List<Post> queuedPosts;

    public AllPostsIterator(String blogName) {
      this.blogName = blogName;
      this.queuedPosts = new LinkedList<Post>();
      this.offset = 0;

      requestPosts();
    }

    @Override
    public boolean hasNext() {
      return !queuedPosts.isEmpty();
    }

    @Override
    public Post next() {
      Post post = queuedPosts.get(0);
      queuedPosts.remove(0);

      if (queuedPosts.isEmpty()) {
        requestPosts();
      }

      return post;
    }

    @Override
    public void remove() {
      throw new NotImplementedException();
    }

    private void requestPosts() {
      Instant retrievedInstant = Instant.now();
      List<com.tumblr.jumblr.types.Post> posts = client.blogPosts(blogName,
              ImmutableMap.of("offset", offset));
      offset += posts.size();

      for (com.tumblr.jumblr.types.Post post : posts) {
        queuedPosts.add(fromJumblrPost(post, retrievedInstant));
      }
    }
  }

  private static final long MILLIS_PER_SECOND = 1000;

  private final JumblrClient client;

  public JumblrTumblrApi(JumblrClient client) {
    this.client = client;
  }

  @Override
  public Iterable<Post> getAllPosts(final String blogName) {
    return new Iterable<Post>() {

      @Override
      public Iterator<Post> iterator() {
        return new AllPostsIterator(blogName);
      }
    };
  }

  @Override
  public Post getPost(String blogName, long id) {
    return fromJumblrPost(client.blogPost(blogName, id));
  }

  private static void fromJumblrAnswerPost(AnswerPost.Builder builder,
          com.tumblr.jumblr.types.AnswerPost post) {
    builder.setAnswer(post.getAnswer());
    builder.setAskingName(post.getAskingName());
    builder.setAskingUrl(post.getAskingUrl());
    builder.setQuestion(post.getQuestion());
  }

  private static void fromJumblrAudioPost(AudioPost.Builder builder,
          com.tumblr.jumblr.types.AudioPost post) {
    builder.setAlbum(post.getAlbumName());
    builder.setAlbumArt(post.getAlbumArtUrl());
    builder.setArtist(post.getArtistName());
    builder.setCaption(post.getCaption());
    builder.setPlayer(post.getSourceUrl()); // TODO(bpitman): is this correct?
    builder.setPlays(post.getPlayCount());
    builder.setTrackName(post.getTrackName());
    builder.setTrackNumber(post.getTrackNumber());
    builder.setYear(post.getYear());
  }

  private static void fromJumblrChatPost(ChatPost.Builder builder,
          com.tumblr.jumblr.types.ChatPost post) {
    ImmutableList.Builder<Dialogue> dialogueBuilder = ImmutableList.builder();
    for (com.tumblr.jumblr.types.Dialogue dialogue : post.getDialogue()) {
      dialogueBuilder.add(new Dialogue(dialogue.getName(), dialogue.getLabel(), dialogue
              .getPhrase()));
    }

    builder.setBody(post.getBody());
    builder.setDialogue(dialogueBuilder.build());
    builder.setTitle(post.getTitle());
  }

  private static void fromJumblrLinkPost(LinkPost.Builder builder,
          com.tumblr.jumblr.types.LinkPost post) {
    builder.setDescription(post.getDescription());
    builder.setTitle(post.getTitle());
    builder.setUrl(post.getLinkUrl());
  }

  private static void fromJumblrPhotoPost(PhotoPost.Builder builder,
          com.tumblr.jumblr.types.PhotoPost post) {
    ImmutableList.Builder<Photo> photosBuilder = ImmutableList.builder();
    for (com.tumblr.jumblr.types.Photo photo : post.getPhotos()) {
      ImmutableList.Builder<PhotoSize> photoSizesBuilder = ImmutableList.builder();
      for (com.tumblr.jumblr.types.PhotoSize photoSize : photo.getSizes()) {
        photoSizesBuilder.add(new PhotoSize(photoSize.getWidth(), photoSize.getHeight(), photoSize
                .getUrl()));
      }

      photosBuilder.add(new Photo(photo.getCaption(), photoSizesBuilder.build()));
    }

    builder.setCaption(post.getCaption());
    builder.setHeight(post.getHeight());
    builder.setPhotos(photosBuilder.build());
    builder.setWidth(post.getWidth());
  }

  private static Post fromJumblrPost(com.tumblr.jumblr.types.Post post) {
    return fromJumblrPost(post, Instant.now());
  }

  private static Post fromJumblrPost(com.tumblr.jumblr.types.Post post, Instant retrievedInstant) {
    Post.Builder builder;

    switch (post.getType()) {
    case "answer":
      builder = new AnswerPost.Builder();
      fromJumblrAnswerPost((AnswerPost.Builder) builder, (com.tumblr.jumblr.types.AnswerPost) post);
      break;
    case "audio":
      builder = new AudioPost.Builder();
      fromJumblrAudioPost((AudioPost.Builder) builder, (com.tumblr.jumblr.types.AudioPost) post);
      break;
    case "chat":
      builder = new ChatPost.Builder();
      fromJumblrChatPost((ChatPost.Builder) builder, (com.tumblr.jumblr.types.ChatPost) post);
      break;
    case "link":
      builder = new LinkPost.Builder();
      fromJumblrLinkPost((LinkPost.Builder) builder, (com.tumblr.jumblr.types.LinkPost) post);
      break;
    case "photo":
      builder = new PhotoPost.Builder();
      fromJumblrPhotoPost((PhotoPost.Builder) builder, (com.tumblr.jumblr.types.PhotoPost) post);
      break;
    case "quote":
      builder = new QuotePost.Builder();
      fromJumblrQuotePost((QuotePost.Builder) builder, (com.tumblr.jumblr.types.QuotePost) post);
      break;
    case "text":
      builder = new TextPost.Builder();
      fromJumblrTextPost((TextPost.Builder) builder, (com.tumblr.jumblr.types.TextPost) post);
      break;
    case "video":
      builder = new VideoPost.Builder();
      fromJumblrVideoPost((VideoPost.Builder) builder, (com.tumblr.jumblr.types.VideoPost) post);
      break;
    default:
      throw new AssertionError(String.format("Post %d has impossible type %s.", post.getId(),
              post.getType()));
    }

    builder.setId(post.getId());
    builder.setBlogName(post.getBlogName());
    builder.setPostUrl(post.getPostUrl());
    builder.setPostedInstant(new Instant(MILLIS_PER_SECOND * post.getTimestamp()));
    builder.setRetrievedInstant(retrievedInstant);
    builder.setTags(post.getTags());

    return builder.build();
  }

  private static void fromJumblrQuotePost(QuotePost.Builder builder,
          com.tumblr.jumblr.types.QuotePost post) {
    builder.setSource(post.getSource());
    builder.setText(post.getText());
  }

  private static void fromJumblrTextPost(TextPost.Builder builder,
          com.tumblr.jumblr.types.TextPost post) {
    builder.setTitle(Strings.nullToEmpty(post.getTitle()));
    builder.setBody(post.getBody());
  }

  private static void fromJumblrVideoPost(VideoPost.Builder builder,
          com.tumblr.jumblr.types.VideoPost post) {
    ImmutableList.Builder<Video> videosBuilder = ImmutableList.builder();
    for (com.tumblr.jumblr.types.Video video : post.getVideos()) {
      videosBuilder.add(new Video(video.getWidth(), video.getEmbedCode()));
    }

    builder.setCaption(post.getCaption());
    builder.setPlayers(videosBuilder.build());
  }
}
