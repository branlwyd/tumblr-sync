package cc.bran.tumblr.types;

import java.util.Collection;
import java.util.Objects;

import org.joda.time.Instant;

import com.google.common.base.Preconditions;

/**
 * Represents a Tumblr quote post.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class QuotePost extends Post {

  private final String source;

  private final String text;

  public QuotePost(long id, String blogName, String postUrl, Instant postedInstant,
          Instant retrievedInstant, Collection<String> tags, String text, String source) {
    super(id, blogName, postUrl, postedInstant, retrievedInstant, tags);
    Preconditions.checkNotNull(text);
    Preconditions.checkNotNull(source);

    this.text = text;
    this.source = source;
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }
    if (!(other instanceof QuotePost)) {
      return false;
    }
    QuotePost otherPost = (QuotePost) other;
    return Objects.equals(this.source, otherPost.source)
            && Objects.equals(this.text, otherPost.text);
  }

  public String getSource() {
    return source;
  }

  public String getText() {
    return text;
  }

  @Override
  public PostType getType() {
    return PostType.QUOTE;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), text, source);
  }
}
