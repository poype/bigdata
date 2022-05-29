package com.poype.bigdata.spark.eighth;

import java.io.Serializable;

public class MovieScore implements Serializable {

    private String userId;

    private String movieId;

    private int score;

    private String timestamp;

    public MovieScore() {
    }

    public MovieScore(String userId, String movieId, int score, String timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.score = score;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "MovieScore{" +
                "userId='" + userId + '\'' +
                ", movieId='" + movieId + '\'' +
                ", score=" + score +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
