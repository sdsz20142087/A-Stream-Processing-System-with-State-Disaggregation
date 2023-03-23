package utils;

import java.io.Serializable;
import java.util.function.Predicate;

public class WIKIPredicate implements Predicate<WikiInfo>, Serializable {

    @Override
    public boolean test(WikiInfo wikiInfo) {
        return wikiInfo.id % 4 != 0;
    }
}
