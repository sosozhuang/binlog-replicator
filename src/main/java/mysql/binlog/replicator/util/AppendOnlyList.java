package mysql.binlog.replicator.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.function.UnaryOperator;

/**
 * An {@link ArrayList} that can only append elements.
 *
 * @param <E> the type of elements in this list
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/10/21.
 */
public final class AppendOnlyList<E> extends ArrayList<E> {
    private static final long serialVersionUID = -2544619381099237741L;

    public AppendOnlyList(int initialCapacity) {
        super(initialCapacity);
    }

    public AppendOnlyList() {
    }

    public AppendOnlyList(Collection<? extends E> c) {
        super(c);
    }

    @Override
    public E set(int index, E element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E remove(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(UnaryOperator<E> operator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sort(Comparator<? super E> c) {
        throw new UnsupportedOperationException();
    }
}
