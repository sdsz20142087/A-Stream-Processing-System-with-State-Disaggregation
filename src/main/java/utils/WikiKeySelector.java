package utils;

import operators.IKeySelector;

public class WikiKeySelector implements IKeySelector {
    @Override
    public String getUniqueKey(Object o) {
        WikiInfo wi = (WikiInfo) o;
        return wi.server_name;
    }
}
