import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable{

    private String starting_phrase;
    private String following_word;
    private int count;

    public DBOutputWritable(String starting_phrase, String following_word, int count) {
        this.starting_phrase = starting_phrase;
        this.following_word = following_word;
        this.count = count;
    }

    public void readFields(ResultSet resultSet) throws SQLException {

        starting_phrase = resultSet.getString(1);
        following_word = resultSet.getString(2);
        count = resultSet.getInt(3);
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        //the 1st, 2nd, 3rd column of table in database is starting_phrase, following_word and count
        preparedStatement.setString(1, starting_phrase);
        preparedStatement.setString(2, following_word);
        preparedStatement.setInt(3, count);
    }
}
