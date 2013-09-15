package cc.bran.tumblr.types;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.joda.time.Instant;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Represents a Tumblr video post.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class VideoPost extends Post {

  public static class Builder extends Post.Builder {

    private String caption;

    private List<Video> players;

    @Override
    public VideoPost build() {
      return new VideoPost(id, blogName, postUrl, postedInstant, retrievedInstant, tags, caption,
              players);
    }

    public void setCaption(String caption) {
      this.caption = caption;
    }

    public void setPlayers(Collection<Video> players) {
      this.players = ImmutableList.copyOf(players);
    }
  }

  public static class Video {

    private final String embedCode;

    private final int width;

    public Video(int width, String embedCode) {
      Preconditions.checkNotNull(embedCode);

      this.width = width;
      this.embedCode = embedCode;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Video)) {
        return false;
      }
      Video otherVideo = (Video) other;
      return Objects.equals(this.width, otherVideo.width)
              && Objects.equals(this.embedCode, otherVideo.embedCode);
    }

    public String getEmbedCode() {
      return embedCode;
    }

    public int getWidth() {
      return width;
    }

    @Override
    public int hashCode() {
      return Objects.hash(width, embedCode);
    }
  }

  private final String caption;

  private final List<Video> players;

  public VideoPost(long id, String blogName, String postUrl, Instant postedInstant,
          Instant retrievedInstant, Collection<String> tags, String caption,
          Collection<Video> players) {
    super(id, blogName, postUrl, postedInstant, retrievedInstant, tags);
    Preconditions.checkNotNull(caption);
    Preconditions.checkNotNull(players);

    this.caption = caption;
    this.players = ImmutableList.copyOf(players);
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }
    if (!(other instanceof VideoPost)) {
      return false;
    }
    VideoPost otherPost = (VideoPost) other;
    return Objects.equals(this.caption, otherPost.caption)
            && Objects.equals(this.players, otherPost.players);
  }

  public String getCaption() {
    return caption;
  }

  public List<Video> getPlayers() {
    return players;
  }

  @Override
  public PostType getType() {
    return PostType.VIDEO;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), caption, players);
  }
}
