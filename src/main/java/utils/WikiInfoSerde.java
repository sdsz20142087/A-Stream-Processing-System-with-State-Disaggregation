package utils;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;

import java.io.Serializable;

public class WikiInfoSerde implements SerDe<WikiInfo>, Serializable {
    @Override
    public WikiInfo deserializeIn(ByteString bs) {
        // convert to string, then load json
        String info = bs.toStringUtf8();
        Gson gson = new Gson();
        WikiInfo wikiInfo = gson.fromJson(info, WikiInfo.class);
        return wikiInfo;
    }

    @Override
    public ByteString serializeOut(WikiInfo wikiInfo) {
        // convert to json, then to ByteString
        Gson gson = new Gson();
        String json = gson.toJson(wikiInfo);
        return ByteString.copyFromUtf8(json);
    }
}
