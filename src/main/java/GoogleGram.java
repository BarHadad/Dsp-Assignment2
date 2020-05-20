import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GoogleGram implements WritableComparable {

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

    public void setGram(String gram) {
        this.gram = gram;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public long getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(long occurrences) {
        this.occurrences = occurrences;
    }

    public long getPages() {
        return pages;
    }

    public void setPages(long pages) {
        this.pages = pages;
    }

    public long getBooks() {
        return books;
    }

    public void setBooks(long books) {
        this.books = books;
    }

    @Override
    public int compareTo(Object o) {
        return gram.compareTo(((GoogleGram)o).gram);
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
        pages =  dataInput.readLong();
        books =  dataInput.readLong();
    }
}
