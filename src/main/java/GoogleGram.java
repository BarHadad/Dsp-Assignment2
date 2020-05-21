import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class GoogleGram implements WritableComparable<GoogleGram> {

    private String gram; // - The n gram
    private String year; // - The year for this aggregation
    private long occurrences; // - The number of times this n-gram appeared in this year
    private long pages; // - The number of pages this n-gram appeared on in this year
    private long books; // - The number of books this n-gram appeared in during this year

    public GoogleGram(String gram, String year, long occurrences, long pages, long books) {
        this.gram = gram;
        this.year = year;
        this.occurrences = occurrences;
        this.pages = pages;
        this.books = books;
    }

    public GoogleGram() {
        gram = null;
        year = null;
        occurrences = -1;
        pages = -1;
        books = -1;
    }

    public String getGram() {
        return gram;
    }

    public String getYear() {
        return year;
    }

    public long getOccurrences() {
        return occurrences;
    }

    public long getPages() {
        return pages;
    }

    public long getBooks() {
        return books;
    }

    @Override
    public int compareTo(GoogleGram o) {
        return gram.compareTo(o.gram);
    }

    /**
     * DataOutput - hadoop special object which include array of bytes
     * with the output inside.
     * Convert to byte, and sent online or how the fuck we want.
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(gram);
        dataOutput.writeUTF(year);
        dataOutput.writeLong(occurrences);
        dataOutput.writeLong(pages);
        dataOutput.writeLong(books);
    }

    /**
     * Read the n gram that sent. help us restore the object we sent online.
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        gram = dataInput.readUTF();
        year = dataInput.readUTF();
        occurrences = dataInput.readLong();
        pages = dataInput.readLong();
        books = dataInput.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GoogleGram that = (GoogleGram) o;
        return occurrences == that.occurrences &&
                pages == that.pages &&
                books == that.books &&
                Objects.equals(gram, that.gram) &&
                Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gram, year, occurrences, pages, books);
    }
}
