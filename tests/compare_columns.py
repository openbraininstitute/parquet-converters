import numpy
import pandas
import tables


def compare(parquet, syn2):
    """Compare a few columns of two files
    """
    df = pandas.read_parquet(parquet)
    t = tables.open_file(syn2, 'r')

    columns = ("post_position", "synapse_id", "spine_length")
    for col in columns:
        if not hasattr(df, col):
            continue
        old = getattr(df, col).iloc[:10]
        if old.dtype == 'O':
            def extract(df, subcol):
                return df.apply(lambda d: d[subcol])
            old = pandas.DataFrame({'x': extract(old, 'x'),
                                    'y': extract(old, 'y'),
                                    'z': extract(old, 'z')})
        now = getattr(t.root.synapses.default.properties, col)[:10]
        equal = numpy.all(numpy.isclose(old, now))
        print(f"{col} is identical: {equal}")
        yield equal
    t.close()


if __name__ == '__main__':
    import os
    import sys
    try:
        pq, syn2 = sys.argv[1:]
    except ValueError:
        print(f"usage: {os.path.basename(sys.argv[0])} parquet syn2")
        sys.exit(1)

    sys.exit(0 if numpy.all(list(compare(pq, syn2))) else 1)
