package cc.bran.tumblr.types;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.joda.time.Instant;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Represents a Tumblr chat post.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class ChatPost extends Post {

  public static class Builder extends Post.Builder {

    private String body;

    private List<Dialogue> dialogue;

    private String title;

    @Override
    public ChatPost build() {
      return new ChatPost(id, blogName, postUrl, postedInstant, retrievedInstant, tags, title,
              body, dialogue);
    }

    public void setBody(String body) {
      this.body = body;
    }

    public void setDialogue(Collection<Dialogue> dialogue) {
      this.dialogue = ImmutableList.copyOf(dialogue);
    }

    public void setTitle(String title) {
      this.title = title;
    }
  }

  public static class Dialogue {

    private final String label;

    private final String name;

    private final String phrase;

    public Dialogue(String name, String label, String phrase) {
      Preconditions.checkNotNull(name);
      Preconditions.checkNotNull(label);
      Preconditions.checkNotNull(phrase);

      this.name = name;
      this.label = label;
      this.phrase = phrase;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Dialogue)) {
        return false;
      }
      Dialogue otherDialogue = (Dialogue) other;
      return Objects.equals(this.name, otherDialogue.name)
              && Objects.equals(this.label, otherDialogue.label)
              && Objects.equals(this.phrase, otherDialogue.phrase);
    }

    public String getLabel() {
      return label;
    }

    public String getName() {
      return name;
    }

    public String getPhrase() {
      return phrase;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, label, phrase);
    }
  }

  private final String body;

  private final List<Dialogue> dialogue;

  private final String title;

  public ChatPost(long id, String blogName, String postUrl, Instant postedInstant,
          Instant retrievedInstant, Collection<String> tags, String title, String body,
          Collection<Dialogue> dialogue) {
    super(id, blogName, postUrl, postedInstant, retrievedInstant, tags);
    Preconditions.checkNotNull(title);
    Preconditions.checkNotNull(body);
    Preconditions.checkNotNull(dialogue);

    this.title = title;
    this.body = body;
    this.dialogue = ImmutableList.copyOf(dialogue);
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }
    if (!(other instanceof ChatPost)) {
      return false;
    }
    ChatPost otherPost = (ChatPost) other;
    return Objects.equals(this.title, otherPost.title) && Objects.equals(this.body, otherPost.body)
            && Objects.equals(this.dialogue, otherPost.dialogue);
  }

  public String getBody() {
    return body;
  }

  public List<Dialogue> getDialogue() {
    return dialogue;
  }

  public String getTitle() {
    return title;
  }

  @Override
  public PostType getType() {
    return PostType.CHAT;
  }

  @Override
  public int hashCode() {
    return Objects.hash(title, body, dialogue);
  }
}
