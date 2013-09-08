package cc.bran.tumblr.types;

import java.util.Collection;
import java.util.Objects;

import org.joda.time.Instant;

import com.google.common.base.Preconditions;

/**
 * Represents a Tumblr audio post.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class AudioPost extends Post {

  private final String album;

  private final String albumArt;

  private final String artist;

  private final String caption;

  private final String player;

  private final int plays;

  private final String trackName;

  private final int trackNumber;

  private final int year;

  public AudioPost(long id, String blogName, String postUrl, Instant postedInstant,
          Instant retrievedInstant, Collection<String> tags, String caption, String player,
          int plays, String albumArt, String artist, String album, String trackName,
          int trackNumber, int year) {
    super(id, blogName, postUrl, postedInstant, retrievedInstant, tags);
    Preconditions.checkNotNull(caption);
    Preconditions.checkNotNull(player);
    Preconditions.checkNotNull(albumArt);
    Preconditions.checkNotNull(artist);
    Preconditions.checkNotNull(album);
    Preconditions.checkNotNull(trackName);

    this.caption = caption;
    this.player = player;
    this.plays = plays;
    this.albumArt = albumArt;
    this.artist = artist;
    this.album = album;
    this.trackName = trackName;
    this.trackNumber = trackNumber;
    this.year = year;
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }
    if (!(other instanceof AudioPost)) {
      return false;
    }
    AudioPost otherPost = (AudioPost) other;
    return Objects.equals(this.caption, otherPost.caption)
            && Objects.equals(this.player, otherPost.player)
            && Objects.equals(this.plays, otherPost.plays)
            && Objects.equals(this.albumArt, otherPost.albumArt)
            && Objects.equals(this.artist, otherPost.artist)
            && Objects.equals(this.album, otherPost.album)
            && Objects.equals(this.trackName, otherPost.trackName)
            && Objects.equals(this.trackNumber, otherPost.trackNumber)
            && Objects.equals(this.year, otherPost.year);
  }

  public String getAlbum() {
    return album;
  }

  public String getAlbumArt() {
    return albumArt;
  }

  public String getArtist() {
    return artist;
  }

  public String getCaption() {
    return caption;
  }

  public String getPlayer() {
    return player;
  }

  public int getPlays() {
    return plays;
  }

  public String getTrackName() {
    return trackName;
  }

  public int getTrackNumber() {
    return trackNumber;
  }

  @Override
  public PostType getType() {
    return PostType.AUDIO;
  }

  public int getYear() {
    return year;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), caption, player, plays, albumArt, artist, album,
            trackName, trackNumber, year);
  }
}
