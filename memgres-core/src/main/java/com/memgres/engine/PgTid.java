package com.memgres.engine;

/**
 * A {@code tid}: where a stored tuple sits, as a block number and a slot within that block.
 *
 * <p>Held as a pair rather than as the text {@code (block,offset)} it prints as, because the two
 * numbers are what a tid is ordered by. Compared as text, {@code (0,10)} sorted before
 * {@code (0,9)} and {@code (1,1)} before {@code (0,9)}, so {@code ORDER BY ctid} handed back rows
 * in an order the table does not have.
 *
 * <p>The widths are PostgreSQL's own: the block is a 32-bit unsigned number and the slot a 16-bit
 * one, which is what makes {@code '(0,70000)'::tid} and {@code '(4294967296,1)'::tid} refusals
 * rather than values.
 */
public final class PgTid implements Comparable<PgTid> {

    private static final long MAX_BLOCK = 4294967295L;
    private static final long MIN_BLOCK = -2147483648L;
    private static final int MAX_OFFSET = 65535;

    private final long block;
    private final int offset;

    public PgTid(long block, int offset) {
        this.block = block;
        this.offset = offset;
    }

    /**
     * Read a tid as PostgreSQL's input function does, or refuse the text as one.
     *
     * <p>Anything the shape does not fit is the same refusal as a number out of range: what the
     * reader wrote is not a tid, and reporting the parts it could make out would be describing a
     * value that was never there.
     *
     * <p>Three things about the shape are PostgreSQL's rather than anyone's choice, and each was
     * measured. Whitespace is skipped before the opening paren and ignored after the closing one —
     * {@code '(0,1)x'} is a tid — but not inside, so {@code '( 0 , 1 )'} is not one. The block is
     * read as a signed number and then taken as the unsigned 32-bit one it is, which is what makes
     * {@code '(-1,1)'} the last block rather than a refusal; the slot is not, and no negative slot
     * is a slot.
     */
    public static PgTid parse(String input) {
        String s = input == null ? "" : input;
        int i = 0;
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        int comma = s.indexOf(',', i);
        int close = comma < 0 ? -1 : s.indexOf(')', comma);
        if (i < s.length() && s.charAt(i) == '(' && comma > i + 1 && close > comma + 1) {
            try {
                long block = Long.parseLong(s.substring(i + 1, comma));
                long offset = Long.parseLong(s.substring(comma + 1, close));
                if (block >= MIN_BLOCK && block <= MAX_BLOCK
                        && offset >= 0 && offset <= MAX_OFFSET) {
                    return new PgTid(block & MAX_BLOCK, (int) offset);
                }
            } catch (NumberFormatException ignored) {
                // falls through to the refusal below, which is what PostgreSQL answers either way
            }
        }
        throw new MemgresException(
                "invalid input syntax for type tid: \"" + (input == null ? "" : input) + "\"",
                "22P02");
    }

    public long block() {
        return block;
    }

    public int offset() {
        return offset;
    }

    @Override
    public int compareTo(PgTid other) {
        int byBlock = Long.compare(block, other.block);
        return byBlock != 0 ? byBlock : Integer.compare(offset, other.offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PgTid)) return false;
        PgTid other = (PgTid) o;
        return block == other.block && offset == other.offset;
    }

    @Override
    public int hashCode() {
        return (int) (block * 31 + offset);
    }

    @Override
    public String toString() {
        return "(" + block + "," + offset + ")";
    }
}
